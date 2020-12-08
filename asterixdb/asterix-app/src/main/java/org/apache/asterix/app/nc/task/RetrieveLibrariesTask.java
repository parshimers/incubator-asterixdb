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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RetrieveLibrariesTask implements INCLifecycleTask {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();
    private final URI libraryURI;
    private String authToken;
    private final String libraryZipName;

    public RetrieveLibrariesTask(URI libraryURI, String authToken) {
        this.libraryURI = libraryURI;
        this.authToken = authToken;
        this.libraryZipName = "all_libraries_" + System.currentTimeMillis() + ".zip";
    }

    @Override
    public void perform(CcId ccId, IControllerService cs) throws HyracksDataException {
        INcApplicationContext appContext = (INcApplicationContext) cs.getApplicationContext();
        ILibraryManager libraryManager = appContext.getLibraryManager();
        FileReference targetFile = appContext.getLibraryManager().getStorageDir().getChild(libraryZipName);
        try {
            libraryManager.download(targetFile, authToken, libraryURI);
            Path outputDirPath = libraryManager.getStorageDir().getFile().toPath().toAbsolutePath().normalize();
            FileReference outPath = appContext.getIoManager().resolveAbsolutePath(outputDirPath.toString());
            appContext.getLibraryManager().unzip(targetFile, outPath);
        } catch (IOException e) {
            LOGGER.error("Unable to retrieve UDFs from " + libraryURI.toString() + " before timeout");
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\" }";
    }
}
