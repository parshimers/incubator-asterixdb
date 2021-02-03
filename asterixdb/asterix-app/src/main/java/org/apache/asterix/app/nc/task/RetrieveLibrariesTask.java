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
package org.apache.asterix.app.nc.task;

import static org.apache.asterix.api.http.server.NCUdfRecoveryServlet.GET_ALL_UDF_ENDPOINT;
import static org.apache.asterix.api.http.server.NCUdfRecoveryServlet.GET_UDF_LIST_ENDPOINT;
import static org.apache.asterix.common.utils.Servlets.UDF_RECOVERY;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RetrieveLibrariesTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private static final int FETCH_RETRY_COUNT = 10;
    private static final Logger LOGGER = LogManager.getLogger();
    private final String libraryZipName;
    private final List<Pair<URI, String>> nodes;
    private final Pair<URI, String> thisNode;
    private final Random rand;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public RetrieveLibrariesTask(List<Pair<URI, String>> nodes, Pair<URI, String> thisNode) {
        this.libraryZipName = "all_libraries_" + System.currentTimeMillis() + ".zip";
        this.nodes = nodes;
        this.thisNode = thisNode;
        this.rand = new Random();
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        if (nodes.size() > 0) {
            boolean success = false;
            for (Pair<URI, String> referenceNode : nodes) {
                try {
                    if (!isUdfStateConsistent(referenceNode, thisNode)) {
                        retrieveLibrary(referenceNode.getFirst(), referenceNode.getSecond(), appContext);
                        success = true;
                        break;
                    }
                } catch (HyracksDataException e) {
                    LOGGER.warn("Unable to retrieve UDFs from: " + referenceNode.getFirst() + ", trying another node.",
                            e);
                }
            }
            if (!success) {
                throw HyracksDataException.create(ErrorCode.TIMEOUT);
            }
        }
    }

    private void retrieveLibrary(URI baseURI, String authToken, INcApplicationContext appContext)
            throws HyracksDataException {
        ILibraryManager libraryManager = appContext.getLibraryManager();
        FileReference distributionDir = appContext.getLibraryManager().getDistributionDir();
        URI libraryURI = getNCUdfRetrievalURL(baseURI);
        try {
            FileUtil.forceMkdirs(distributionDir.getFile());
            Path targetFile = Files.createTempFile(Paths.get(distributionDir.getAbsolutePath()), "all_", ".zip");
            FileReference targetFileRef = distributionDir.getChild(targetFile.getFileName().toString());
            libraryManager.download(targetFileRef, authToken, libraryURI);
            Path outputDirPath = libraryManager.getStorageDir().getFile().toPath().toAbsolutePath().normalize();
            FileReference outPath = appContext.getIoManager().resolveAbsolutePath(outputDirPath.toString());
            libraryManager.unzip(targetFileRef, outPath);
        } catch (IOException e) {
            LOGGER.error("Unable to retrieve UDFs from " + libraryURI.toString() + " before timeout");
            throw HyracksDataException.create(e);
        }
    }

    private boolean isUdfStateConsistent(Pair<URI, String> existingNode, Pair<URI, String> incomingNode) {
        try {
            String existingList = getUdfState(getNCUdfListingURL(existingNode.first),
                    Collections.singletonMap(HttpHeaders.AUTHORIZATION, existingNode.second));
            String incomingList = getUdfState(getNCUdfListingURL(incomingNode.first),
                    Collections.singletonMap(HttpHeaders.AUTHORIZATION, incomingNode.second));
            LOGGER.debug("Existing libraries: " + existingList);
            LOGGER.debug("Incoming libraries:" + incomingList);
            if (incomingList != null && existingList != null) {
                JsonNode existing = OBJECT_MAPPER.readTree(existingList);
                JsonNode incoming = OBJECT_MAPPER.readTree(incomingList);
                return existing.equals(incoming);
            } else {
                return incomingList == existingList;
            }
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, e);
            //fall through
        }
        return false;
    }

    //TODO: this could be refactored with the UDF download code so it could either write to a file
    //      or return a string instead of having to dupe a lot of the download logic
    private String getUdfState(URI state, Map<String, String> addtlHeaders) throws IOException {
        // retry 10 times at maximum for downloading binaries
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpGet request = new HttpGet(state);
            for (Map.Entry<String, String> e : addtlHeaders.entrySet()) {
                request.setHeader(e.getKey(), e.getValue());
            }
            int tried = 0;
            Exception trace = null;
            while (tried < FETCH_RETRY_COUNT) {
                tried++;
                CloseableHttpResponse response = null;
                try {
                    response = httpClient.execute(request);
                    if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        throw new IOException("Http Error: " + response.getStatusLine().getStatusCode());
                    }
                    HttpEntity e = response.getEntity();
                    if (e == null) {
                        throw new IOException("No response");
                    }
                    return IOUtils.toString(e.getContent(), "UTF-8");
                } catch (IOException e) {
                    LOGGER.error("Unable to download library", e);
                    trace = e;
                } finally {
                    if (response != null) {
                        try {
                            response.close();
                        } catch (IOException e) {
                            LOGGER.warn("Failed to close", e);
                        }
                    }
                }
            }
            LOGGER.error(trace);
        }
        return null;
    }

    public URI getNCUdfRetrievalURL(URI baseURL) {
        String endpoint = UDF_RECOVERY.substring(0, UDF_RECOVERY.length() - 1) + GET_ALL_UDF_ENDPOINT;
        URIBuilder builder = new URIBuilder(baseURL).setPath(endpoint);
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            LOGGER.error("Could not find URL for NC recovery", e);
        }
        return null;
    }

    public URI getNCUdfListingURL(URI baseURL) {
        String endpoint = UDF_RECOVERY.substring(0, UDF_RECOVERY.length() - 1) + GET_UDF_LIST_ENDPOINT;
        URIBuilder builder = new URIBuilder(baseURL).setPath(endpoint);
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            LOGGER.error("Could not find URL for NC recovery", e);
        }
        return null;
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
