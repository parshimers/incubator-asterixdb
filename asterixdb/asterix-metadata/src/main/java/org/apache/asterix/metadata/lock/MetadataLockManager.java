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
package org.apache.asterix.metadata.lock;

import static org.apache.asterix.common.metadata.MetadataEntityKind.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.IMetadataLock;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.common.metadata.MetadataLockKey;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class MetadataLockManager implements IMetadataLockManager {

    private static final Function<MetadataLockKey, MetadataLock> LOCK_FUNCTION = MetadataLock::new;
    private static final Function<MetadataLockKey, DatasetLock> DATASET_LOCK_FUNCTION = DatasetLock::new;

    private final ConcurrentMap<MetadataLockKey, IMetadataLock> mdlocks;

    public MetadataLockManager() {
        mdlocks = new ConcurrentHashMap<>();
    }

    @Override
    public void acquireDataverseReadLock(LockList locks, DataverseName dataverseName) throws AlgebricksException {
        MetadataLockKey key = createDataverseLockKey(dataverseName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataverseWriteLock(LockList locks, DataverseName dataverseName) throws AlgebricksException {
        MetadataLockKey key = createDataverseLockKey(dataverseName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetReadLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDatasetWriteLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDatasetModifyLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.MODIFY, lock);
    }

    @Override
    public void acquireDatasetCreateIndexLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.INDEX_BUILD, lock);
    }

    @Override
    public void acquireDatasetExclusiveModificationLock(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

    @Override
    public void acquireFunctionReadLock(LockList locks, DataverseName dataverseName, String functionName)
            throws AlgebricksException {
        MetadataLockKey key = createFunctionLockKey(dataverseName, functionName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireFunctionWriteLock(LockList locks, DataverseName dataverseName, String functionName)
            throws AlgebricksException {
        MetadataLockKey key = createFunctionLockKey(dataverseName, functionName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireNodeGroupReadLock(LockList locks, String nodeGroupName) throws AlgebricksException {
        MetadataLockKey key = createNodeGroupLockKey(nodeGroupName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireNodeGroupWriteLock(LockList locks, String nodeGroupName) throws AlgebricksException {
        MetadataLockKey key = createNodeGroupLockKey(nodeGroupName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireActiveEntityReadLock(LockList locks, DataverseName dataverseName, String entityName)
            throws AlgebricksException {
        MetadataLockKey key = createActiveEntityLockKey(dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireActiveEntityWriteLock(LockList locks, DataverseName dataverseName, String entityName)
            throws AlgebricksException {
        MetadataLockKey key = createActiveEntityLockKey(dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyWriteLock(LockList locks, DataverseName dataverseName, String feedPolicyName)
            throws AlgebricksException {
        MetadataLockKey key = createFeedPolicyLockKey(dataverseName, feedPolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireFeedPolicyReadLock(LockList locks, DataverseName dataverseName, String feedPolicyName)
            throws AlgebricksException {
        MetadataLockKey key = createFeedPolicyLockKey(dataverseName, feedPolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyReadLock(LockList locks, String mergePolicyName) throws AlgebricksException {
        MetadataLockKey key = createMergePolicyLockKey(mergePolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireMergePolicyWriteLock(LockList locks, String mergePolicyName) throws AlgebricksException {
        MetadataLockKey key = createMergePolicyLockKey(mergePolicyName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireDataTypeReadLock(LockList locks, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        MetadataLockKey key = createDataTypeLockKey(dataverseName, datatypeName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireDataTypeWriteLock(LockList locks, DataverseName dataverseName, String datatypeName)
            throws AlgebricksException {
        MetadataLockKey key = createDataTypeLockKey(dataverseName, datatypeName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void acquireExtensionEntityReadLock(LockList locks, String extension, DataverseName dataverseName,
            String entityName) throws AlgebricksException {
        MetadataLockKey key = createExtensionEntityLockKey(extension, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.READ, lock);
    }

    @Override
    public void acquireExtensionEntityWriteLock(LockList locks, String extension, DataverseName dataverseName,
            String entityName) throws AlgebricksException {
        MetadataLockKey key = createExtensionEntityLockKey(extension, dataverseName, entityName);
        IMetadataLock lock = mdlocks.computeIfAbsent(key, LOCK_FUNCTION);
        locks.add(IMetadataLock.Mode.WRITE, lock);
    }

    @Override
    public void upgradeDatasetLockToWrite(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.upgrade(IMetadataLock.Mode.UPGRADED_WRITE, lock);
    }

    @Override
    public void downgradeDatasetLockToExclusiveModify(LockList locks, DataverseName dataverseName, String datasetName)
            throws AlgebricksException {
        MetadataLockKey key = createDatasetLockKey(dataverseName, datasetName);
        DatasetLock lock = (DatasetLock) mdlocks.computeIfAbsent(key, DATASET_LOCK_FUNCTION);
        locks.downgrade(IMetadataLock.Mode.EXCLUSIVE_MODIFY, lock);
    }

    private static MetadataLockKey createDataverseLockKey(DataverseName dataverseName) {
        return new MetadataLockKey(DATAVERSE, null, dataverseName, null);
    }

    private static MetadataLockKey createDatasetLockKey(DataverseName dataverseName, String datasetName) {
        return new MetadataLockKey(DATASET, null, dataverseName, datasetName);
    }

    private MetadataLockKey createDataTypeLockKey(DataverseName dataverseName, String datatypeName) {
        return new MetadataLockKey(DATATYPE, null, dataverseName, datatypeName);
    }

    private MetadataLockKey createFunctionLockKey(DataverseName dataverseName, String functionName) {
        return new MetadataLockKey(FUNCTION, null, dataverseName, functionName);
    }

    private MetadataLockKey createActiveEntityLockKey(DataverseName dataverseName, String entityName) {
        return new MetadataLockKey(ACTIVE, null, dataverseName, entityName);
    }

    private MetadataLockKey createFeedPolicyLockKey(DataverseName dataverseName, String feedPolicyName) {
        return new MetadataLockKey(FEED_POLICY, null, dataverseName, feedPolicyName);
    }

    private MetadataLockKey createExtensionEntityLockKey(String extension, DataverseName dataverseName,
            String entityName) {
        return new MetadataLockKey(EXTENSION, extension, dataverseName, entityName);
    }

    private MetadataLockKey createNodeGroupLockKey(String nodeGroupName) {
        return new MetadataLockKey(NODE_GROUP, null, null, nodeGroupName);
    }

    private MetadataLockKey createMergePolicyLockKey(String mergePolicyName) {
        return new MetadataLockKey(MERGE_POLICY, null, null, mergePolicyName);
    }
}
