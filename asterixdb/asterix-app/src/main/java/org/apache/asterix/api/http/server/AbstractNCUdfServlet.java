/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.JAVA;
import static org.apache.asterix.common.functions.ExternalFunctionLanguage.PYTHON;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.control.common.work.SynchronizableWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public abstract class AbstractNCUdfServlet extends AbstractServlet {

    INcApplicationContext appCtx;
    INCServiceContext srvCtx;

    protected final IApplicationContext plainAppCtx;
    private final HttpScheme httpServerProtocol;
    private final int httpServerPort;

    public static final String GET_UDF_DIST_ENDPOINT = "/dist";
    public static final String DATAVERSE_PARAMETER = "dataverse";
    public static final String NAME_PARAMETER = "name";
    public static final String TYPE_PARAMETER = "type";
    public static final String DELETE_PARAMETER = "delete";
    public static final String IFEXISTS_PARAMETER = "ifexists";
    public static final String DATA_PARAMETER = "data";

    protected enum LibraryOperation {
        UPSERT,
        DELETE
    }

    protected final static class LibraryUploadData {

        final LibraryOperation op;
        final DataverseName dataverse;
        final String name;
        final ExternalFunctionLanguage type;
        final boolean replaceIfExists;
        final FileUpload fileUpload;

        private LibraryUploadData(MixedAttribute dataverse, MixedAttribute name, MixedAttribute type,
                InterfaceHttpData fileUpload) throws IOException {
            op = LibraryOperation.UPSERT;
            this.dataverse = DataverseName.createFromCanonicalForm(dataverse.getValue());
            this.name = name.getValue();
            this.type = getLanguageByTypeParameter(type.getValue());
            this.replaceIfExists = true;
            this.fileUpload = (FileUpload) fileUpload;
        }

        private LibraryUploadData(MixedAttribute dataverse, MixedAttribute name, boolean replaceIfExists)
                throws IOException {
            op = LibraryOperation.DELETE;
            this.dataverse = DataverseName.createFromCanonicalForm(dataverse.getValue());
            this.name = name.getValue();
            this.type = null;
            this.replaceIfExists = replaceIfExists;
            this.fileUpload = null;
        }

        public static LibraryUploadData libraryCreationUploadData(MixedAttribute dataverse, MixedAttribute name,
                MixedAttribute type, InterfaceHttpData fileUpload) throws IOException {
            return new LibraryUploadData(dataverse, name, type, fileUpload);
        }

        public static LibraryUploadData libraryDeletionUploadData(MixedAttribute dataverse, MixedAttribute name,
                boolean replaceIfExists) throws IOException {
            return new LibraryUploadData(dataverse, name, replaceIfExists);
        }
    }

    public AbstractNCUdfServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            HttpScheme httpServerProtocol, int httpServerPort) {

        super(ctx, paths);
        this.plainAppCtx = appCtx;
        this.httpServerProtocol = httpServerProtocol;
        this.httpServerPort = httpServerPort;
    }

    void readFromFile(Path filePath, IServletResponse response, String contentType, OpenOption opt) throws Exception {
        class InputStreamGetter extends SynchronizableWork {
            private InputStream is;

            @Override
            protected void doRun() throws Exception {
                if (opt != null) {
                    is = Files.newInputStream(filePath, opt);
                } else {
                    is = Files.newInputStream(filePath);
                }
            }
        }

        InputStreamGetter r = new InputStreamGetter();
        ((NodeControllerService) srvCtx.getControllerService()).getWorkQueue().scheduleAndSync(r);

        if (r.is == null) {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return;
        }
        try {
            response.setStatus(HttpResponseStatus.OK);
            HttpUtil.setContentType(response, contentType);
            IOUtils.copyLarge(r.is, response.outputStream());
        } finally {
            r.is.close();
        }
    }

    URI createDownloadURI(Path file) throws Exception {
        String path = paths[0].substring(0, trims[0]) + GET_UDF_DIST_ENDPOINT + '/' + file.getFileName();
        String host = getHyracksClientConnection().getHost();
        return new URI(httpServerProtocol.toString(), null, host, httpServerPort, path, null, null);
    }

    IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        if (hcc == null) {
            throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
        }
        return hcc;
    }

    protected String getDataverseParameter() {
        return DATAVERSE_PARAMETER;
    }

    protected LibraryUploadData decodeMultiPartLibraryOptions(HttpPostRequestDecoder requestDecoder)
            throws IOException {
        MixedAttribute dataverse = (MixedAttribute) requestDecoder.getBodyHttpData(getDataverseParameter());
        MixedAttribute name = (MixedAttribute) requestDecoder.getBodyHttpData(NAME_PARAMETER);
        MixedAttribute type = (MixedAttribute) requestDecoder.getBodyHttpData(TYPE_PARAMETER);
        MixedAttribute delete = (MixedAttribute) requestDecoder.getBodyHttpData(DELETE_PARAMETER);
        MixedAttribute replaceIfExists = (MixedAttribute) requestDecoder.getBodyHttpData(IFEXISTS_PARAMETER);
        if (dataverse == null) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, getDataverseParameter());
        } else if (name == null) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, NAME_PARAMETER);
        } else if ((type == null && delete == null)) {
            throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED,
                    TYPE_PARAMETER + " or " + DELETE_PARAMETER);
        } else if (type != null && delete != null) {
            throw RuntimeDataException.create(ErrorCode.INVALID_PARAM_COMBO, TYPE_PARAMETER, DELETE_PARAMETER);
        }
        if (delete != null) {
            boolean replace = false;
            if (replaceIfExists != null) {
                replace = Boolean.TRUE.toString().equalsIgnoreCase(replaceIfExists.getValue());
            }
            return LibraryUploadData.libraryDeletionUploadData(dataverse, name, replace);
        } else {
            InterfaceHttpData libraryData = requestDecoder.getBodyHttpData(DATA_PARAMETER);
            if (libraryData == null) {
                throw RuntimeDataException.create(ErrorCode.PARAMETERS_REQUIRED, DATA_PARAMETER);
            } else if (!libraryData.getHttpDataType().equals(InterfaceHttpData.HttpDataType.FileUpload)) {
                throw RuntimeDataException.create(ErrorCode.INVALID_REQ_PARAM_VAL, DATA_PARAMETER,
                        libraryData.getHttpDataType());
            }
            LibraryUploadData uploadData =
                    LibraryUploadData.libraryCreationUploadData(dataverse, name, type, libraryData);
            if (uploadData.type == null) {
                throw RuntimeDataException.create(ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNSUPPORTED_KIND,
                        type.getValue());
            }
            return uploadData;
        }
    }

    static ExternalFunctionLanguage getLanguageByTypeParameter(String lang) {
        if (lang.equalsIgnoreCase(JAVA.name())) {
            return JAVA;
        } else if (lang.equalsIgnoreCase(PYTHON.name())) {
            return PYTHON;
        } else {
            return null;
        }
    }

    HttpResponseStatus toHttpErrorStatus(Exception e) {
        if (IFormattedException.matchesAny(e, ErrorCode.UNKNOWN_DATAVERSE, ErrorCode.UNKNOWN_LIBRARY)) {
            return HttpResponseStatus.NOT_FOUND;
        }
        if (IFormattedException.matchesAny(e, ErrorCode.LIBRARY_EXTERNAL_FUNCTION_UNKNOWN_KIND,
                ErrorCode.INVALID_REQ_PARAM_VAL, ErrorCode.PARAMETERS_REQUIRED, ErrorCode.INVALID_PARAM_COMBO)) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        if (e instanceof AlgebricksException) {
            return HttpResponseStatus.BAD_REQUEST;
        }
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }

}
