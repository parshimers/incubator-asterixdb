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
package org.apache.asterix.transaction.management.service.transaction;

import org.apache.asterix.common.config.AsterixReplicationProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;
import org.apache.asterix.common.config.IAsterixPropertiesProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.asterix.transaction.management.service.locking.ConcurrentLockManager;
import org.apache.asterix.transaction.management.service.logging.LogManager;
import org.apache.asterix.transaction.management.service.recovery.CheckpointThread;
import org.apache.asterix.transaction.management.service.recovery.RecoveryManager;
import org.apache.commons.logging.Log;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.zookeeper.Op;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Provider for all the sub-systems (transaction/lock/log/recovery) managers.
 * Users of transaction sub-systems must obtain them from the provider.
 */
public class TransactionSubsystem implements ITransactionSubsystem {
    private final String id;
    private final ILockManager lockManager;
    private final ITransactionManager transactionManager;
    private final IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider;
    private final AsterixTransactionProperties txnProperties;
    final Map<Set<Integer>, Triple<LogManager, RecoveryManager, CheckpointThread>> partitionToLoggerMap;
    final Triple<LogManager, RecoveryManager, CheckpointThread> baseLogger;

    public TransactionSubsystem(String id, IAsterixAppRuntimeContextProvider asterixAppRuntimeContextProvider,
            AsterixTransactionProperties txnProperties, PersistentLocalResourceRepository localResourceRepository)
            throws ACIDException {
        this.asterixAppRuntimeContextProvider = asterixAppRuntimeContextProvider;
        this.id = id;
        this.txnProperties = txnProperties;
        this.transactionManager = new TransactionManager(this);
        this.lockManager = new ConcurrentLockManager(txnProperties.getLockManagerShrinkTimer());

        LogManager baseLogManager = new LogManager(this);
        RecoveryManager baseRecoveryManager = new RecoveryManager(this);
        CheckpointThread baseCheckpointThread = null;

        if (asterixAppRuntimeContextProvider != null) {
            baseCheckpointThread = new CheckpointThread(baseRecoveryManager,
                    asterixAppRuntimeContextProvider.getDatasetLifecycleManager(), baseLogManager,
                    this.txnProperties.getCheckpointLSNThreshold(), this.txnProperties.getCheckpointPollFrequency());
            baseCheckpointThread.start();
        }

        partitionToLoggerMap = new HashMap<>();
        baseLogger = new Triple<>(baseLogManager, baseRecoveryManager, baseCheckpointThread);
        partitionToLoggerMap.put(localResourceRepository.getNodeOrignalPartitions(), baseLogger);

    }

    private Triple<LogManager, RecoveryManager, CheckpointThread> getTxnInfoByPartition(int partition) {
        Set<Set<Integer>> partitions = partitionToLoggerMap.keySet();
        for (Set s : partitions) {
            if (s.contains(partition)) {
                return partitionToLoggerMap.get(s);
            }
        }
        return null;
    }

    @Override
    public ILogManager getLogManager(Set<Integer> partitionIds) {
        return partitionToLoggerMap.get(partitionIds).first;
    }

    @Override
    public ILogManager getLogManager(int partitionId) {
        Triple<LogManager, RecoveryManager, CheckpointThread> txnInfo = getTxnInfoByPartition(partitionId);
        if (txnInfo != null) {
            return txnInfo.first;
        } else
            return null;
    }

    @Override
    public ILogManager getLogManager(IRecoveryManager recoveryManager) {
        for (Triple<LogManager, RecoveryManager, CheckpointThread> t : partitionToLoggerMap.values()) {
            if (t.second.equals(recoveryManager))
                return t.first;
        }
        return null;
    }

    @Override
    public ILogManager getBaseLogManager() {
        return baseLogger.first;
    }

    public ILockManager getLockManager() {
        return lockManager;
    }

    public ITransactionManager getTransactionManager() {
        return transactionManager;
    }

    @Override
    public IRecoveryManager getRecoveryManager(Set<Integer> partitionIds) {
        return partitionToLoggerMap.get(partitionIds).second;
    }

    @Override
    public IRecoveryManager getRecoveryManager(int partitionId) {
        Triple<LogManager, RecoveryManager, CheckpointThread> txnInfo = getTxnInfoByPartition(partitionId);
        if (txnInfo != null) {
            return txnInfo.second;
        } else
            return null;
    }

    @Override
    public IRecoveryManager getRecoveryManager(ILogManager logManager) {
        for (Triple<LogManager, RecoveryManager, CheckpointThread> t : partitionToLoggerMap.values()) {
            if (t.first.equals(logManager))
                return t.second;
        }
        return null;
    }

    @Override
    public IRecoveryManager getBaseRecoveryManager() {
        return baseLogger.second;
    }

    public IAsterixAppRuntimeContextProvider getAsterixAppRuntimeContextProvider() {
        return asterixAppRuntimeContextProvider;
    }

    public AsterixTransactionProperties getTransactionProperties() {
        return txnProperties;
    }

    public String getId() {
        return id;
    }

}
