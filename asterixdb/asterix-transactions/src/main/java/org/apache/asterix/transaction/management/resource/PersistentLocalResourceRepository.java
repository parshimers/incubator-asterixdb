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
import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.config.AsterixMetadataProperties;
import org.apache.asterix.common.replication.AsterixReplicationJob;
import org.apache.asterix.common.replication.IReplicationManager;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.control.nc.io.IOManager;
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
    private final String[] mountPoints;
    private static final String STORAGE_METADATA_DIRECTORY = "asterix_root_metadata";
    private static final String STORAGE_METADATA_FILE_NAME_PREFIX = ".asterix_root_metadata";
    private static final long STORAGE_LOCAL_RESOURCE_ID = -4321;
    public static final String METADATA_FILE_NAME = ".metadata";
    private final Cache<String, LocalResource> resourceCache;
    private final String nodeId;
    private static final int MAX_CACHED_RESOURCES = 1000;
    private IReplicationManager replicationManager;
    private boolean isReplicationEnabled = false;
    private Set<String> filesToBeReplicated;
    private IIOManager ioManager;
    private final SortedMap<Integer, ClusterPartition> clusterPartitions;
    private final Set<Integer> nodeOriginalPartitions;
    private final Set<Integer> nodeActivePartitions;
    private Set<Integer> nodeInactivePartitions;

    public PersistentLocalResourceRepository(List<IODeviceHandle> devices, String nodeId,
                                             AsterixMetadataProperties metadataProperties, IIOManager ioManager) throws HyracksDataException {
        mountPoints = new String[devices.size()];
        this.ioManager = ioManager;
        this.nodeId = nodeId;
        this.clusterPartitions = metadataProperties.getClusterPartitions();
        for (int i = 0; i < mountPoints.length; i++) {
            String mountPoint = devices.get(i).getPath().getPath();
            FileReference mountPointDir = new FileReference(mountPoint,
                    FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
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

        ClusterPartition[] nodePartitions = metadataProperties.getNodePartitions().get(nodeId);
        //initially the node active partitions are the same as the original partitions
        nodeOriginalPartitions = new HashSet<>(nodePartitions.length);
        nodeActivePartitions = new HashSet<>(nodePartitions.length);
        nodeInactivePartitions = ConcurrentHashMap.newKeySet();
        for (ClusterPartition partition : nodePartitions) {
            nodeOriginalPartitions.add(partition.getPartitionId());
            nodeActivePartitions.add(partition.getPartitionId());
        }
    }

    private static String getStorageMetadataDirPath(String mountPoint, String nodeId, int ioDeviceId) {
        return mountPoint + STORAGE_METADATA_DIRECTORY + File.separator + nodeId + "_" + "iodevice" + ioDeviceId;
    }

    private FileReference getStorageMetadataBaseDir(FileReference storageMetadataFile) {
        //STORAGE_METADATA_DIRECTORY / Node Id / STORAGE_METADATA_FILE_NAME_PREFIX
        return ioManager.getParent(ioManager.getParent(storageMetadataFile));
    }

    public void initializeNewUniverse(String storageRootDirName) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Initializing local resource repository ... ");
        }

        //create storage metadata file (This file is used to locate the root storage directory after instance restarts).
        //TODO with the existing cluster configuration file being static and distributed on all NCs, we can find out the storage root
        //directory without looking at this file. This file could potentially store more information, otherwise no need to keep it.
        for (int i = 0; i < mountPoints.length; i++) {
            FileReference storageMetadataFile = getStorageMetadataFile(mountPoints[i], nodeId, i);
            FileReference storageMetadataDir = ioManager.getParent(storageMetadataFile);
            //make dirs for the storage metadata file
            if (!ioManager.exists(storageMetadataDir)) {
                boolean success = ioManager.mkdirs(storageMetadataDir);
                if (!success) {
                    throw new IllegalStateException(
                            "Unable to create storage metadata directory of PersistentLocalResourceRepository in "
                                    + storageMetadataDir.getAbsolutePath() + " or directory already exists");
                }
            }

            LOGGER.log(Level.INFO,
                    "created the root-metadata-file's directory: " + storageMetadataDir.getAbsolutePath());

            String storageRootDirPath;
            if (storageRootDirName.startsWith(System.getProperty("file.separator"))) {
                storageRootDirPath = new String(
                        mountPoints[i] + storageRootDirName.substring(System.getProperty("file.separator").length()));
            } else {
                storageRootDirPath = new String(mountPoints[i] + storageRootDirName);
            }

            LocalResource rootLocalResource = new LocalResource(STORAGE_LOCAL_RESOURCE_ID,
                    storageMetadataFile.getAbsolutePath(), 0, storageMetadataFile.getAbsolutePath(), 0,
                    storageRootDirPath);
            insert(rootLocalResource);
            LOGGER.log(Level.INFO, "created the root-metadata-file: " + storageMetadataFile.getAbsolutePath());
        }
        LOGGER.log(Level.INFO, "Completed the initialization of the local resource repository");
    }

    @Override
    public LocalResource getResourceByPath(String path) throws HyracksDataException {
        LocalResource resource = resourceCache.getIfPresent(path);
        if (resource == null) {
            FileReference resourceFile = getLocalResourceFileByName(path);
            if (ioManager.exists(resourceFile)) {
                resource = readLocalResource(resourceFile);
                resourceCache.put(path, resource);
            }
        }
        return resource;
    }

    @Override
    public synchronized void insert(LocalResource resource) throws HyracksDataException {
        FileReference resourceFile = new FileReference(
                getFileName(resource.getResourcePath(), resource.getResourceId()),
                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        if (ioManager.exists(resourceFile)) {
            throw new HyracksDataException("Duplicate resource: " + resourceFile.getAbsolutePath());
        } else if (!ioManager.exists(ioManager.getParent(resourceFile))){
            ioManager.mkdirs(ioManager.getParent(resourceFile));
        }

        if (resource.getResourceId() != STORAGE_LOCAL_RESOURCE_ID) {
            resourceCache.put(resource.getResourcePath(), resource);
        }
        ByteArrayOutputStream fos = null;
        ObjectOutputStream oosToFos = null;

        try {
            fos = new ByteArrayOutputStream();
            oosToFos = new ObjectOutputStream(fos);
            oosToFos.writeObject(resource);
            oosToFos.flush();
            IFileHandle fh = ioManager.open(resourceFile, IIOManager.FileReadWriteMode.READ_WRITE,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            ioManager.append(fh, ByteBuffer.wrap(fos.toByteArray()));
            ioManager.sync(fh, true);
            ioManager.close(fh);

        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

        //if replication enabled, send resource metadata info to remote nodes
    }

    @Override
    public synchronized void deleteResourceByPath(String resourcePath) throws HyracksDataException {
        FileReference resourceFile = getLocalResourceFileByName(resourcePath);
        if (ioManager.exists(resourceFile)) {
            ioManager.delete(resourceFile);
            resourceCache.invalidate(resourcePath);

            //if replication enabled, delete resource from remote replicas
            if (isReplicationEnabled && !resourceFile.getName().startsWith(STORAGE_METADATA_FILE_NAME_PREFIX)) {
                createReplicationJob(ReplicationOperation.DELETE, resourceFile.getAbsolutePath());
            }
        } else {
            throw new HyracksDataException("Resource doesn't exist");
        }
    }

    private static FileReference getLocalResourceFileByName(String resourcePath) {
        return new FileReference(resourcePath + File.separator + METADATA_FILE_NAME,
                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
    }

    public HashMap<Long, LocalResource> loadAndGetAllResources() throws HyracksDataException {
        //TODO During recovery, the memory usage currently is proportional to the number of resources available.
        //This could be fixed by traversing all resources on disk until the required resource is found.
        HashMap<Long, LocalResource> resourcesMap = new HashMap<Long, LocalResource>();

        for (int i = 0; i < mountPoints.length; i++) {
            FileReference storageRootDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageRootDir == null) {
                continue;
            }

            //load all local resources that belong to us.
            List<String> partitions = new ArrayList<>(
                    Arrays.asList(ioManager.listFiles(storageRootDir, NOOP_FILES_FILTER)));
            for (String candidatePart : Arrays.asList(ioManager.listFiles(storageRootDir,NOOP_FILES_FILTER))) {
                if(!nodeActivePartitions.contains(Integer.parseInt(candidatePart.split("_")[1]))){
                        partitions.remove(candidatePart);
                }
            }
            for (String sPartition : partitions) {
                FileReference partition = new FileReference(sPartition,
                        FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                String[] dataverseFileList = ioManager.listFiles(partition, NOOP_FILES_FILTER);
                if (dataverseFileList != null) {
                    for (String sDataverseFile : dataverseFileList) {
                        FileReference dataverseFile = new FileReference(sDataverseFile,
                                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                        if (ioManager.isDirectory(dataverseFile)) {
                            String[] indexFileList = ioManager.listFiles(dataverseFile, NOOP_FILES_FILTER);
                            if (indexFileList != null) {
                                for (String sIndexFile : indexFileList) {
                                    FileReference indexFile = new FileReference(sIndexFile,
                                            FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                                    if (ioManager.isDirectory(indexFile)) {
                                        String[] metadataFiles = ioManager.listFiles(indexFile, METADATA_FILES_FILTER);
                                        if (metadataFiles != null) {
                                            for (String sMetadataFile : metadataFiles) {
                                                FileReference metadataFile = new FileReference(sMetadataFile,
                                                        FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
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

        for (int i = 0; i < mountPoints.length; i++) {
            FileReference storageRootDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageRootDir == null) {
                continue;
            }

            //load all local resources.
            String[] partitions = ioManager.listFiles(storageRootDir, NOOP_FILES_FILTER);
            for (String sPartition : partitions) {
                FileReference partition = new FileReference(sPartition,
                        FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                String[] dataverseFileList = ioManager.listFiles(partition, NOOP_FILES_FILTER);
                if (dataverseFileList != null) {
                    for (String sDataverseFile : dataverseFileList) {
                        FileReference dataverseFile = new FileReference(sDataverseFile,
                                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                        if (ioManager.isDirectory(dataverseFile)) {
                            String[] indexFileList = ioManager.listFiles(dataverseFile, NOOP_FILES_FILTER);
                            if (indexFileList != null) {
                                for (String sIndexFile : indexFileList) {
                                    FileReference indexFile = new FileReference(sIndexFile,
                                            FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                                    if (ioManager.isDirectory(indexFile)) {
                                        String[] metadataFiles = ioManager.listFiles(indexFile, METADATA_FILES_FILTER);
                                        if (metadataFiles != null) {
                                            for (String sMetadataFile : metadataFiles) {
                                                FileReference metadataFile = new FileReference(sMetadataFile,
                                                        FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
                                                LocalResource localResource = readLocalResource(metadataFile);
                                                maxResourceId = Math.max(maxResourceId, localResource.getResourceId());
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

    private static String getFileName(String baseDir, long resourceId) {
        if (resourceId == STORAGE_LOCAL_RESOURCE_ID) {
            return baseDir;
        } else {
            if (!baseDir.endsWith(System.getProperty("file.separator"))) {
                baseDir += System.getProperty("file.separator");
            }
            return new String(baseDir + METADATA_FILE_NAME);
        }
    }

    public LocalResource readLocalResource(FileReference file) throws HyracksDataException {
        FileInputStream fis = null;
        ObjectInputStream oisFromFis = null;

        try {
            IFileHandle fHandle = ioManager.open(file, IIOManager.FileReadWriteMode.READ_ONLY,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);
            oisFromFis = new ObjectInputStream(ioManager.getInputStream(fHandle));
            LocalResource resource = (LocalResource) oisFromFis.readObject();
            return resource;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private static final FilenameFilter METADATA_FILES_FILTER = new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
            if (name.equalsIgnoreCase(METADATA_FILE_NAME)) {
                return true;
            } else {
                return false;
            }
        }
    };

    private static final FilenameFilter NOOP_FILES_FILTER = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return true;
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
        /**
         * Durable resources path format:
         * /partition/dataverse/idx/fileName
         * Temporary resources path format:
         * /partition/TEMP_DATASETS_STORAGE_FOLDER/dataverse/idx/fileName
         */
        String[] fileNameTokens = filePath.split(File.separator);
        String partitionDir = fileNameTokens[fileNameTokens.length - 4];
        //exclude temporary datasets resources
        if (!partitionDir.equals(StoragePathUtil.TEMP_DATASETS_STORAGE_FOLDER)) {
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
    }

    public String[] getStorageMountingPoints() {
        return mountPoints;
    }

    /**
     * Deletes physical files of all data verses.
     *
     * @param deleteStorageMetadata
     * @throws IOException
     */
    public void deleteStorageData(boolean deleteStorageMetadata) throws IOException {
        for (int i = 0; i < mountPoints.length; i++) {
            FileReference storageDir = getStorageRootDirectoryIfExists(mountPoints[i], nodeId, i);
            if (storageDir != null) {
                if (ioManager.isDirectory(storageDir)) {
                    ioManager.delete(storageDir, true);
                }
            }

            if (deleteStorageMetadata) {
                //delete the metadata root directory
                FileReference storageMetadataFile = getStorageMetadataFile(mountPoints[i], nodeId, i);
                FileReference storageMetadataDir = getStorageMetadataBaseDir(storageMetadataFile);
                if (ioManager.exists(storageMetadataDir) && ioManager.isDirectory(storageMetadataDir)) {
                    ioManager.delete(storageMetadataDir, true);
                }
            }
        }
    }

    /**
     * @param mountPoint
     * @param nodeId
     * @param ioDeviceId
     * @return A file reference to the storage metadata file.
     */
    private static FileReference getStorageMetadataFile(String mountPoint, String nodeId, int ioDeviceId) {
        String storageMetadataFileName = getStorageMetadataDirPath(mountPoint, nodeId, ioDeviceId) + File.separator
                + STORAGE_METADATA_FILE_NAME_PREFIX;
        FileReference storageMetadataFile = new FileReference(storageMetadataFileName,
                FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        return storageMetadataFile;
    }

    /**
     * @param mountPoint
     * @param nodeId
     * @param ioDeviceId
     * @return A file reference to the storage root directory if exists, otherwise null.
     * @throws HyracksDataException
     */
    public FileReference getStorageRootDirectoryIfExists(String mountPoint, String nodeId, int ioDeviceId)
            throws HyracksDataException {
        FileReference storageRootDir = null;
        FileReference storageMetadataFile = getStorageMetadataFile(mountPoint, nodeId, ioDeviceId);
        if (ioManager.exists(storageMetadataFile)) {
            LocalResource rootLocalResource = readLocalResource(storageMetadataFile);
            String storageRootDirPath = (String) rootLocalResource.getResourceObject();
            storageRootDir = new FileReference(storageRootDirPath,
                    FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
        }
        return storageRootDir;
    }

    /**
     * @param partition
     * @return The partition local path on this NC.
     */
    public String getPartitionPath(int partition) {
        //currently each partition is replicated on the same IO device number on all NCs.
        return mountPoints[getIODeviceNum(partition)];
    }

    public int getIODeviceNum(int partition) {
        return clusterPartitions.get(partition).getIODeviceNum();
    }

    public Set<Integer> getActivePartitions() {
        return Collections.unmodifiableSet(nodeActivePartitions);
    }

    public Set<Integer> getInactivePartitions() {
        return Collections.unmodifiableSet(nodeInactivePartitions);
    }

    public Set<Integer> getNodeOrignalPartitions() {
        return Collections.unmodifiableSet(nodeOriginalPartitions);
    }

    public synchronized void addActivePartition(int partitonId) {
        nodeActivePartitions.add(partitonId);
        nodeInactivePartitions.remove(partitonId);
    }

    public synchronized void addInactivePartition(int partitonId) {
        nodeInactivePartitions.add(partitonId);
        nodeActivePartitions.remove(partitonId);
    }

    /**
     * @param resourceAbsolutePath
     * @return the resource relative path starting from the partition directory
     */
    public static String getResourceRelativePath(String resourceAbsolutePath) {
        String[] tokens = resourceAbsolutePath.split(File.separator);
        //partition/dataverse/idx/fileName
        return tokens[tokens.length - 4] + File.separator + tokens[tokens.length - 3] + File.separator
                + tokens[tokens.length - 2] + File.separator + tokens[tokens.length - 1];
    }

    public static int getResourcePartition(String resourceAbsolutePath) {
        String[] tokens = resourceAbsolutePath.split(File.separator);
        //partition/dataverse/idx/fileName
        return StoragePathUtil.getPartitionNumFromName(tokens[tokens.length - 4]);
    }
}
