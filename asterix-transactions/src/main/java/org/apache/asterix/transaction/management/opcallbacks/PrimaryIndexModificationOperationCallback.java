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

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.dataflow.AsterixLSMInsertDeleteOperatorNodePushable;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILockManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;

/**
 * Assumes LSM-BTrees as primary indexes.
 * Performs locking on primary keys, and also logs before/after images.
 */
public class PrimaryIndexModificationOperationCallback extends AbstractIndexModificationOperationCallback implements
        IModificationOperationCallback {

    // For the temporary experiment
    //    Random randomValueGenerator = null;
    private final AsterixLSMInsertDeleteOperatorNodePushable operatorNodePushable;

    public PrimaryIndexModificationOperationCallback(int datasetId, int[] primaryKeyFields, ITransactionContext txnCtx,
            ILockManager lockManager, ITransactionSubsystem txnSubsystem, long resourceId, byte resourceType,
            IndexOperation indexOp, IOperatorNodePushable operatorNodePushable) {
        super(datasetId, primaryKeyFields, txnCtx, lockManager, txnSubsystem, resourceId, resourceType, indexOp);
        //        this.randomValueGenerator = new Random(System.currentTimeMillis());
        this.operatorNodePushable = (AsterixLSMInsertDeleteOperatorNodePushable) operatorNodePushable;
    }

    @Override
    public void before(ITupleReference tuple) throws HyracksDataException {
        int pkHash = computePrimaryKeyHashValue(tuple, primaryKeyFields);
        try {
            if (indexOp == IndexOperation.INSERT && operatorNodePushable != null) {
                
                /**********************************************************************************
                 * In order to avoid deadlock during insert operation, the following logic is implemented.
                 * See https://issues.apache.org/jira/browse/ASTERIXDB-1138 for more details regarding deadlock issue
                 * This is not applied to metadata indexes. 
                 * 
                 * 1. for each entry in a frame
                 * 2. returnValue = tryLock() for an entry
                 * 3. if returnValue == false
                 *  3-1. flush all entries which acquired locks so far to the next operator
                 *      : this will make all those entries reach commit operator so that corresponding commit logs will be created.
                 *  3-2. create a WAIT log and wait until logFlusher thread will flush the log and gives notification
                 *      : this notification guarantees that all locks acquired by this transactor were released.
                 *  3-3. acquire lock using lock() instead of tryLock() for the failed entry
                 *      : we know for sure this lock call will not cause deadlock since the transactor doesn't hold any other locks.
                 * 4. create an update log and insert the entry
                 * 
                 * From the above logic, step 2 and 3 are implemented in this before method. 
                 **********************/
                
                //approach 1: release all locks held by this actor (which is a thread) by flushing partial frame. 
                boolean retVal = lockManager.tryLock(jobThreadId, datasetId, pkHash, LockMode.X, txnCtx);
                if (!retVal) {
                    //flush entries which have been inserted already to release locks hold by them
                    operatorNodePushable.flushPartialFrame();
                    
                    //create WAIT log and wait until the WAIT log is flushed and notified by LogFlusher thread
                    logWait();
                    
                    //acquire lock
                    lockManager.lock(jobThreadId, datasetId, pkHash, LockMode.X, txnCtx);
                }
                
                //approach 2: exponential backup
//                long sleepTime = 0;
//                int tryCount = 0;
//                while (!lockManager.tryLock(datasetId, pkHash, LockMode.X, txnCtx)){
//                    if (tryCount == 0) {
//                        //flush entries which have been inserted already to release locks hold by them
//                        operatorNodePushable.flushPartialFrame();
//                        
//                        //create WAIT log and wait until the WAIT log is flushed and notified by LogFlusher thread
//                        logWait();
//                    } else {
//                        try {
//                            Thread.sleep(sleepTime);
//                        } catch (InterruptedException e) {
//                            //ignore
//                        }
//                        if (sleepTime == 0) {
//                            sleepTime = 1;
//                        } else {
//                            sleepTime = sleepTime * 2;
//                        }
//                    }
//                    tryCount++;
//                }
                
            } else {
                lockManager.lock(jobThreadId, datasetId, pkHash, LockMode.X, txnCtx);
            }
            // Temporary for the experiment
            //            try {
            //                Thread.sleep(randomValueGenerator.nextInt(501));// QUERY
            //            } catch (InterruptedException e1) {
            //                // TODO Auto-generated catch block
            //                e1.printStackTrace();
            //            }
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
        try {
            int pkHash = computePrimaryKeyHashValue(after, primaryKeyFields);
            log(pkHash, after);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
    
    private void logWait() throws ACIDException {
        logRecord.setLogType(LogType.WAIT);
        logRecord.computeAndSetLogSize();
        txnSubsystem.getLogManager().log(logRecord);
    }
}
