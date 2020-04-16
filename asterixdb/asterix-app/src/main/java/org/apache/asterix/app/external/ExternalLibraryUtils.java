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

import java.io.FilenameFilter;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Set;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
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

import com.google.common.collect.ImmutableSet;

public class ExternalLibraryUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final FilenameFilter nonHiddenFileNameFilter = (dir, name) -> !name.startsWith(".");
    private static final Set<String> pythonExtensions =
            new ImmutableSet.Builder<String>().add(".pyz").add(".py").build();
    private static final Set<String> javaExtensions =
            new ImmutableSet.Builder<String>().add(".zip").add(".jar").build();

    private ExternalLibraryUtils() {
    }

    /**
     * Remove the library from metadata completely.
     * TODO Currently, external libraries only include functions and adapters. we need to extend this to include:
     * 1. external data source
     * 2. data parser
     *
     * @param dataverse
     * @param libraryName
     * @return true if the library was found and removed, false otherwise
     * @throws AsterixException
     * @throws RemoteException
     * @throws ACIDException
     */
    public static boolean uninstallLibraryFromMetadata(DataverseName dataverse, String libraryName)
            throws AsterixException, RemoteException, ACIDException {
        MetadataTransactionContext mdTxnCtx = null;
        try {
            // begin transaction
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            // make sure dataverse exists
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                return false;
            }
            // make sure library exists
            Library library = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
            if (library == null) {
                return false;
            }

            // get dataverse functions
            List<Function> functions = MetadataManager.INSTANCE.getDataverseFunctions(mdTxnCtx, dataverse);
            for (Function function : functions) {
                // does function belong to library?
                if (function.isExternal() && libraryName.equals(function.getLibrary())) {
                    // drop the function
                    MetadataManager.INSTANCE.dropFunction(mdTxnCtx,
                            new FunctionSignature(dataverse, function.getName(), function.getArity()));
                }
            }

            // get the dataverse adapters
            List<DatasourceAdapter> adapters = MetadataManager.INSTANCE.getDataverseAdapters(mdTxnCtx, dataverse);
            for (DatasourceAdapter adapter : adapters) {
                // belong to the library?
                if (adapter.getAdapterIdentifier().getName().equals(adapter.getLibrary())) {
                    // remove adapter <! we didn't check if there are feeds which use this adapter>
                    MetadataManager.INSTANCE.dropAdapter(mdTxnCtx, dataverse, adapter.getAdapterIdentifier().getName());
                }
            }
            // drop the library itself
            MetadataManager.INSTANCE.dropLibrary(mdTxnCtx, dataverse, libraryName);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw new AsterixException(e);
        }
        return true;
    }

    public static void addLibraryToMetadata(DataverseName dataverse, String libraryName)
            throws ACIDException, RemoteException {
        MetadataTransactionContext mdTxnCtx = null;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            Library libraryInMetadata = MetadataManager.INSTANCE.getLibrary(mdTxnCtx, dataverse, libraryName);
            if (libraryInMetadata != null) {
                // exists in metadata and was not un-installed, we return.
                // Another place which shows that our metadata transactions are broken
                // (we didn't call commit before!!!)
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                return;
            }

            // Add library
            MetadataManager.INSTANCE.addLibrary(mdTxnCtx, new Library(dataverse, libraryName));
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Added library " + libraryName + " to Metadata");
            }

            // Get the dataverse
            Dataverse dv = MetadataManager.INSTANCE.getDataverse(mdTxnCtx, dataverse);
            if (dv == null) {
                MetadataManager.INSTANCE.addDataverse(mdTxnCtx, new Dataverse(dataverse,
                        NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT, MetadataUtil.PENDING_NO_OP));
            }

            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (LOGGER.isErrorEnabled()) {
                LOGGER.log(Level.ERROR, "Exception in installing library " + libraryName, e);
            }
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
        }
    }

}
