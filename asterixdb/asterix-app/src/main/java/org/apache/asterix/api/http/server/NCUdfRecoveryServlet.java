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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpScheme;

public class NCUdfRecoveryServlet extends AbstractNCUdfServlet {

    ExternalLibraryManager libraryManager;

    public static final String GET_ALL_UDF_ENDPOINT = "/all";
    public static final String GET_UDF_LIST_ENDPOINT = "/list";

    private static final Logger LOGGER = LogManager.getLogger();

    public NCUdfRecoveryServlet(ConcurrentMap<String, Object> ctx, String[] paths, IApplicationContext appCtx,
            HttpScheme httpServerProtocol, int httpServerPort) {
        super(ctx, paths, appCtx, httpServerProtocol, httpServerPort);
    }

    @Override
    public void init() throws IOException {
        appCtx = (INcApplicationContext) plainAppCtx;
        srvCtx = this.appCtx.getServiceContext();
        this.libraryManager = (ExternalLibraryManager) appCtx.getLibraryManager();
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        String localPath = localPath(request);
        if (localPath.equals(GET_ALL_UDF_ENDPOINT)) {
            HttpUtil.setContentType(response, HttpUtil.ContentType.ZIP, request);
            Path zippedLibs = libraryManager.zipAllLibs();
            readFromFile(zippedLibs, response);
        } else if (localPath.equals(GET_UDF_LIST_ENDPOINT)) {
            List<Pair<DataverseName, String>> libs = libraryManager.getLibraryListing();
            ObjectNode resp = OBJECT_MAPPER.createObjectNode();
            for (Pair<DataverseName, String> lib : libs) {
                //TODO: slash is not a correct separator, but it is a sin we already commit with the path
                resp.put(lib.first.getCanonicalForm() + "/" + lib.getSecond(),
                        libraryManager.getLibraryHash(lib.first, lib.second));
            }
            response.setStatus(HttpResponseStatus.OK);
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
            PrintWriter responseWriter = response.writer();
            JSONUtil.writeNode(responseWriter, resp);
            responseWriter.flush();

        }
    }
}
