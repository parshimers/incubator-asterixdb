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
package org.apache.asterix.transaction.management.resource;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.replication.AsterixReplicationJob;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.hyracks.storage.common.file.ILocalResourceRepository;
import org.apache.hyracks.storage.common.file.LocalResource;
import org.apache.hyracks.storage.common.file.ResourceIdFactory;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationExecutionType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationJobType;
import org.apache.hyracks.api.replication.IReplicationJob.ReplicationOperation;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PersistentLocalResourceRepository implements ILocalResourceRepository {

    private static final Logger LOGGER = Logger.getLogger(PersistentLocalResourceRepository.class.getName());
    private String[] mountPoints;
    private static final String ROOT_METADATA_DIRECTORY = "asterix_root_metadata";
    private static final String ROOT_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata";
    private static final long ROOT_LOCAL_RESOURCE_ID = -4321;
    public static final String METADATA_FILE_NAME = ".metadata";
    private final Cache<String, LocalResource> resourceCache;
    private final String nodeId;
    private static final int MAX_CACHED_RESOURCES = 1000;
    private IReplicationManager replicationManager;
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IIOManager ioManager;
    
    public PersistentLocalResourceRepository(List<IODeviceHandle> devices, String nodeId) throws HyracksDataException {
        mountPoints = new String[devices.size()];
        this.nodeId = nodeId;
        for (int i = 0; i < mountPoints.length; i++) {
            String mountPoint = devices.get(i).getPath().getPath();
            FileReference mountPointDir = new FileReference(mountPoint, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (!ioManager.exists(mountPointDir)) {
                throw new HyracksDataException(mountPointDir.getPath() + " doesn't exist.");
            }
            if (!mountPoint.endsWith(System.getProperty("file.separator"))) {
                mountPoints[i] = new String(mountPoint + System.getProperty("file.separator"));
            } else {
                mountPoints[i] = new String(mountPoint);
            }
        }

        resourceCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHED_RESOURCES).build();
    }

    private String prepareRootMetaDataFileName(String mountPoint, String nodeId, int ioDeviceId) {
        return mountPoint + ROOT_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice" + ioDeviceId;
    }

    public void initialize(String nodeId, String rootDir) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }

        //if the rootMetadataFile doesn't exist, create it.
        for (int i = 0; i < mountPoints.length; i++) {
            String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                    + ROOT_METADATA_FILE_NAME_PREFIX;
            FileReference rootMetadataFile = new FileReference(rootMetadataFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);

            FileReference rootMetadataDir = new FileReference(prepareRootMetaDataFileName(mountPoints[i], nodeId, i), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (!ioManager.exists(rootMetadataDir)) {
                boolean success = ioManager.mkdirs(rootMetadataDir);
                if (!success) {
                    throw new IllegalStateException(
                            "Unable to create root metadata directory of PersistentLocalResourceRepository in "
                                    + rootMetadataDir.getPath());
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("created the root-metadata-file's directory: " + rootMetadataDir.getPath());
                }
            }

            ioManager.delete(rootMetadataFile);
            String mountedRootDir;
            if (rootDir.startsWith(System.getProperty("file.separator"))) {
                mountedRootDir = new String(mountPoints[i]
                        + rootDir.substring(System.getProperty("file.separator").length()));
            } else {
                mountedRootDir = new String(mountPoints[i] + rootDir);
            }
            LocalResource rootLocalResource = new LocalResource(ROOT_LOCAL_RESOURCE_ID, rootMetadataFileName, 0, 0,
                    mountedRootDir);
            insert(rootLocalResource);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("created the root-metadata-file: " + rootMetadataFileName);
            }
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Completed the initialization of the local resource repository");
        }
    }

    @Override
    public LocalResource getResourceByName(String name) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(name);
        if (resource == null) {
            File resourceFile = getLocalResourceFileByName(name);
            if (resourceFile.exists()) {
                resource = readLocalResource(resourceFile);
                resourceCache.put(name, resource);
            }
        }
        return resource;
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        FileReference resourceFile = new FileReference(getFileName(resource.getResourceName(), resource.getResourceId()), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);

        if (ioManager.exists(resourceFile)) {
            throw new HyracksDataException("Duplicate resource");
        }

        if (resource.getResourceId() != ROOT_LOCAL_RESOURCE_ID) {
            resourceCache.put(resource.getResourceName(), resource);
        }
        ByteArrayOutputStream fos = null;
        ObjectOutputStream oosToFos = null;

        try {
            fos = new ByteArrayOutputStream();
            oosToFos = new ObjectOutputStream(fos);
            oosToFos.writeObject(resource);
            oosToFos.flush();
            FileReference file = new FileReference(getFileName(resource.getResourceName(), resource.getResourceId()), FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if(ioManager.exists(file)){
                ioManager.delete(file);
            }
            IFileHandle fh = ioManager.open(file, IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            ioManager.close(fh);
            fh = ioManager.open(file,IIOManager.FileReadWriteMode.READ_WRITE, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);getFileName(resource.getResourceName(), resource.getResourceId());
            ioManager.append(fh,ByteBuffer.wrap(fos.toByteArray()));
            ioManager.close(fh);

        } catch (IOException e) {
            throw new HyracksDataException(e);
        } finally {
            if (oosToFos != null) {
                try {
                    oosToFos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (oosToFos == null && fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            //if replication enabled, send resource metadata info to remote nodes
            if (isReplicationEnabled && resource.getResourceId() != ROOT_LOCAL_RESOURCE_ID) {
                String filePath = getFileName(resource.getResourceName(), resource.getResourceId());
                createReplicationJob(ReplicationOperation.REPLICATE, filePath);
            }
        }
    }

    @Override
    public synchronized void deleteResourceByName(String name) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(name);
        if (ioManager.exists(resourceFile)) {
            ioManager.delete(resourceFile);
            resourceCache.invalidate(name);
            
            //if replication enabled, delete resource from remote replicas
            if (isReplicationEnabled && !resourceFile.getName().startsWith(ROOT_METADATA_FILE_NAME_PREFIX)) {
                createReplicationJob(ReplicationOperation.DELETE, resourceFile.getAbsolutePath());
            }
        } else {
            throw new HyracksDataException("Resource doesn't exist");
        }
    }

    private static FileReference getLocalResourceFileByName(String resourceName) {
        return new FileReference(resourceName + File.separator + METADATA_FILE_NAME, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
    }

    public HashMap<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        //TODO During recovery, the memory usage currently is proportional to the number of resources available.
        //This could be fixed by traversing all resources on disk until the required resource is found.
        HashMap<Long, LocalResource> resourcesMap = new HashMap<Long, LocalResource>();

        String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                + ROOT_METADATA_FILE_NAME_PREFIX;
        FileReference rootMetadataFile = new FileReference(rootMetadataFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        //#. if the rootMetadataFileReference exists, read it and set this.rootDir.
        LocalResource rootLocalResource = readLocalResource(rootMetadataFile);
        String mountedRootDir = (String) rootLocalResource.getResourceObject();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("The root directory of the local resource repository is " + mountedRootDir);
        }

        //#. load all local resources. 
        FileReference rootDirFile = new FileReference(mountedRootDir, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        if (!ioManager.exists(rootDirFile)) {
            //rootDir may not exist if this node is not the metadata node and doesn't have any user data.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The root directory of the local resource repository doesn't exist: there is no local resource.");
                LOGGER.info("Completed the initialization of the local resource repository");
            }
            continue;
        }

        String[] dataverseFileList = ioManager.listFiles(rootDirFile,METADATA_FILES_FILTER);
        if (dataverseFileList == null) {
            throw new HyracksDataException("Metadata dataverse doesn't exist.");
        }
        for (String dataverseFileName : dataverseFileList) {
            FileReference dataverseFileReference = new FileReference(dataverseFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (ioManager.isDirectory(dataverseFileReference)) {
                String[] indexFileList = ioManager.listFiles(dataverseFileReference, METADATA_FILES_FILTER);
                if (indexFileList != null) {
                    for (String indexFileReferenceName : indexFileList) {
                        FileReference indexFileReference = new FileReference(dataverseFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                        if (ioManager.isDirectory(indexFileReference)) {
                            String[] ioDevicesList = ioManager.listFiles(indexFileReference, METADATA_FILES_FILTER);
                            if (ioDevicesList != null) {
                                for (String ioDeviceFileReferenceName : ioDevicesList) {
                                    FileReference ioDeviceFileReference = new FileReference(ioDeviceFileReferenceName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                                    if (ioManager.isDirectory(ioDeviceFileReference)) {
                                        String[] metadataFiles = ioManager.listFiles(ioDeviceFileReference, METADATA_FILES_FILTER);
                                        if (metadataFiles != null) {
                                            for (File metadataFile : metadataFiles) {
                                                LocalResource localResource = readLocalResource(metadataFile);
                                                resourcesMap.put(localResource.getResourceId(), localResource);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return resourcesMap;
    }

    @Override
    public long getMaxResourceID() throws HyracksDataException {
        long maxResourceId = 0;

        String rootMetadataFileName = prepareRootMetaDataFileName(mountPoints[i], nodeId, i) + File.separator
                + ROOT_METADATA_FILE_NAME_PREFIX;
        FileReference rootMetadataFile = new FileReference(rootMetadataFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        //#. if the rootMetadataFileReference exists, read it and set this.rootDir.
        LocalResource rootLocalResource = readLocalResource(rootMetadataFile);
        String mountedRootDir = (String) rootLocalResource.getResourceObject();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("The root directory of the local resource repository is " + mountedRootDir);
        }

        //#. load all local resources. 
        FileReference rootDirFile = new FileReference(mountedRootDir, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        if (!ioManager.exists(rootDirFile)) {
            //rootDir may not exist if this node is not the metadata node and doesn't have any user data.
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("The root directory of the local resource repository doesn't exist: there is no local resource.");
                LOGGER.info("Completed the initialization of the local resource repository");
            }
            continue;
        }

        String[] dataverseFileList = ioManager.listFiles(rootDirFile,METADATA_FILES_FILTER);
        if (dataverseFileList == null) {
            throw new HyracksDataException("Metadata dataverse doesn't exist.");
        }
        for (String dataverseFileName : dataverseFileList) {
            FileReference dataverseFileReference = new FileReference(dataverseFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            if (ioManager.isDirectory(dataverseFileReference)) {
                String[] indexFileList = ioManager.listFiles(dataverseFileReference,METADATA_FILES_FILTER);
                if (indexFileList != null) {
                    for (String indexFileReferenceName : indexFileList) {
                        FileReference indexFileReference = new FileReference(dataverseFileName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                        if (ioManager.isDirectory(indexFileReference)) {
                            String[] ioDevicesList = ioManager.listFiles(indexFileReference,METADATA_FILES_FILTER);
                            if (ioDevicesList != null) {
                                for (String ioDeviceFileReferenceName : ioDevicesList) {
                                    FileReference ioDeviceFileReference = new FileReference(ioDeviceFileReferenceName, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                                    if (ioManager.isDirectory(ioDeviceFileReference)) {
                                        String[] metadataFiles = ioManager.listFiles(ioDeviceFileReference,filter);
                                        if (metadataFiles != null) {
                                            for (String metadataFileReferenceName : metadataFiles) {
                                                    LocalResource localResource = readLocalResource(metadataFile);
                                                    maxResourceId = Math.max(maxResourceId,
                                                            localResource.getResourceId());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return maxResourceId;
    }

    private String getFileName(String baseDir, long resourceId) {
        if (resourceId == ROOT_LOCAL_RESOURCE_ID) {
            return baseDir;
        } else {
            if (!baseDir.endsWith(System.getProperty("file.separator"))) {
                baseDir += System.getProperty("file.separator");
            }
            String fileName = new String(baseDir + METADATA_FILE_NAME);
            return fileName;
        }
    }

    public static LocalResource readLocalResource(FileReference file) throws HyracksDataException {
        FileInputStream fis = null;
        ObjectInputStream oisFromFis = null;

        try {
            IFileHandle fHandle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY, IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            oisFromFis = new ObjectInputStream(ioManager.getInputStream(fHandle));
            LocalResource resource = (LocalResource) oisFromFis.readObject();
            return resource;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            if (oisFromFis != null) {
                try {
                    oisFromFis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (oisFromFis == null && fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    private static final FilenameFilter METADATA_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            if (name.equalsIgnoreCase(METADATA_FILE_NAME)) {
                return true;
            } else {
                return false;
            }
        }
    };
    public void setReplicationManager(IReplicationManager replicationManager) {
        this.replicationManager = replicationManager;
        isReplicationEnabled = replicationManager.isReplicationEnabled();

        if (isReplicationEnabled) {
            filesToBeReplicated = new HashSet<String>();
        }
    }

    private void createReplicationJob(ReplicationOperation operation, String filePath) throws HyracksDataException {
        filesToBeReplicated.clear();
        filesToBeReplicated.add(filePath);
        AsterixReplicationJob job = new AsterixReplicationJob(ReplicationJobType.METADATA, operation,
                ReplicationExecutionType.SYNC, filesToBeReplicated);
        try {
            replicationManager.submitJob(job);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public String[] getStorageMountingPoints() {
        return mountPoints;
    }

}
