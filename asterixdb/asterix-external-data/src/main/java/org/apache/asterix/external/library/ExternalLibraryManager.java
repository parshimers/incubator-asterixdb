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
package org.apache.asterix.external.library;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibrary;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.om.functions.ExternalFunctionLanguage;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalLibraryManager implements ILibraryManager {

    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Pair<DataverseName, String>, ILibrary> libraries = new ConcurrentHashMap<>();

    public ExternalLibraryManager(File appDir) throws FileNotFoundException {

        File[] libs = appDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return dir.isDirectory();
            }
        });
        if (libs != null) {
            for (File lib : libs) {
                String libraryPath = lib.getAbsolutePath();
                setUpDeployedLibrary(libraryPath);
            }
        }
    }

    public void register(DataverseName dataverseName, String libraryName, ILibrary library) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        libraries.put(key, library);
    }

    @Override
    public void deregister(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        ILibrary cl = libraries.get(key);
        if (cl != null) {
            cl.close();
            libraries.remove(key);
        }
    }

    public void setUpDeployedLibrary(String libraryPath) {
        String[] pathSplit = libraryPath.split("\\.");
        String[] dvSplit = pathSplit[pathSplit.length - 2].split("/");
        DataverseName dataverse = DataverseName.createSinglePartName(dvSplit[dvSplit.length - 1]); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        String name = pathSplit[pathSplit.length - 1].trim();
        try {
            File langFile = new File(libraryPath, "LANG");
            FileInputStream fo = new FileInputStream(langFile);
            String langStr = IOUtils.toString(fo);
            ExternalFunctionLanguage libLang = ExternalFunctionLanguage.valueOf(langStr);
            switch (libLang) {
                case JAVA:
                    register(dataverse, name, new JavaLibrary(libraryPath));
                    break;
                case PYTHON:
                    register(dataverse, name, new PythonLibrary(libraryPath));
                    break;
            }
        } catch (IOException | AsterixException e) {

        }
    }

    @Override
    public <T> ILibrary<T> getLibrary(DataverseName dataverseName, String libraryName) {
        Pair<DataverseName, String> key = getKey(dataverseName, libraryName);
        return libraries.get(key);
    }

    private static Pair<DataverseName, String> getKey(DataverseName dataverseName, String libraryName) {
        return new Pair(dataverseName, libraryName);
    }

}
