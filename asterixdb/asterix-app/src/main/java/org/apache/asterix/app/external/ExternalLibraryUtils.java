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
package org.apache.asterix.app.external;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.DatasourceAdapter;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.entities.Library;
import org.apache.asterix.metadata.utils.MetadataUtil;
import org.apache.asterix.runtime.formats.NonTaggedDataFormat;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExternalLibraryUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final FilenameFilter nonHiddenFileNameFilter = (dir, name) -> !name.startsWith(".");

    private ExternalLibraryUtils() {
    }

    public static void setUpExternaLibrary(ILibraryManager externalLibraryManager, String libraryPath) throws Exception {
        // get the installed library dirs
        String[] pathSplit = libraryPath.split("\\.");
        String[] dvSplit = pathSplit[pathSplit.length - 2].split("/");
        DataverseName dataverse = DataverseName.createSinglePartName(dvSplit[dvSplit.length - 1]); //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        String name = pathSplit[pathSplit.length - 1].trim();
        registerClassLoader(externalLibraryManager, dataverse, name, libraryPath);
    }

    public static void setUpInstalledLibraries(ILibraryManager externalLibraryManager,
            File appDir) throws Exception {
        File[] libs = appDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return dir.isDirectory();
            }
        });
        if (libs != null) {
            for (File lib : libs) {
                setUpExternaLibrary(externalLibraryManager, lib.getAbsolutePath());
            }
        }
    }

    /**
     * register the library class loader with the external library manager
     *
     * @param dataverse
     * @param libraryPath
     * @throws Exception
     */
    protected static void registerClassLoader(ILibraryManager externalLibraryManager, DataverseName dataverse,
            String name, String libraryPath) throws Exception {
        // get the class loader
        URLClassLoader classLoader = getLibraryClassLoader(dataverse, name, libraryPath);
        // register it with the external library manager
        externalLibraryManager.registerLibraryClassLoader(dataverse, name, classLoader);
    }

    /**
     * Get the class loader for the library
     *
     * @param dataverse
     * @param libraryPath
     * @return
     * @throws Exception
     */
    private static URLClassLoader getLibraryClassLoader(DataverseName dataverse, String name, String libraryPath)
            throws Exception {
        // Get a reference to the library directory
        File installDir = new File(libraryPath);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Installing lirbary " + name + " in dataverse " + dataverse + "." + " Install Directory: "
                    + installDir.getAbsolutePath());
        }

        // get a reference to the specific library dir
        File libDir = installDir;

        FilenameFilter jarFileFilter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        };

        // Get the jar file <Allow only a single jar file>
        String[] jarsInLibDir = libDir.list(jarFileFilter);
        if (jarsInLibDir.length > 1) {
            throw new Exception("Incorrect library structure: found multiple library jars");
        }
        if (jarsInLibDir.length <= 0) {
            throw new Exception("Incorrect library structure: could not find library jar");
        }

        File libJar = new File(libDir, jarsInLibDir[0]);
        // get the jar dependencies
        File libDependencyDir = new File(libDir.getAbsolutePath() + File.separator + "lib");
        int numDependencies = 1;
        String[] libraryDependencies = null;
        if (libDependencyDir.exists()) {
            libraryDependencies = libDependencyDir.list(jarFileFilter);
            numDependencies += libraryDependencies.length;
        }

        ClassLoader parentClassLoader = ExternalLibraryUtils.class.getClassLoader();
        URL[] urls = new URL[numDependencies];
        int count = 0;
        // get url of library
        urls[count++] = libJar.toURI().toURL();

        // get urls for dependencies
        if (libraryDependencies != null && libraryDependencies.length > 0) {
            for (String dependency : libraryDependencies) {
                File file = new File(libDependencyDir + File.separator + dependency);
                urls[count++] = file.toURI().toURL();
            }
        }

        if (LOGGER.isInfoEnabled()) {
            StringBuilder logMesg = new StringBuilder("Classpath for library " + dataverse + ": ");
            for (URL url : urls) {
                logMesg.append(url.getFile() + File.pathSeparatorChar);
            }
            LOGGER.info(logMesg.toString());
        }

        // create and return the class loader
        return new ExternalLibraryClassLoader(urls, parentClassLoader);
    }

    /**
     * @return the directory "System.getProperty("app.home", System.getProperty("user.home")/lib/udfs/uninstall"
     */
    protected static File getLibraryUninstallDir() {
        return new File(System.getProperty("app.home", System.getProperty("user.home")) + File.separator + "lib"
                + File.separator + "udfs" + File.separator + "uninstall");
    }

    public static String getExternalFunctionFullName(String libraryName, String functionName) {
        return libraryName + "#" + functionName;
    }

}
