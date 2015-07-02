/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.asterix.transaction.management.opcallbacks;

import org.apache.asterix.common.context.ITransactionSubsystemProvider;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.AbstractOperationCallback;
import org.apache.asterix.common.transactions.AbstractOperationCallbackFactory;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionSubsystem;
import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexLifecycleManager;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class TempDatasetPrimaryIndexModificationOperationCallbackFactory extends AbstractOperationCallbackFactory implements
        IModificationOperationCallbackFactory {

    private static final long serialVersionUID = 1L;
    private final IndexOperation indexOp;

    public TempDatasetPrimaryIndexModificationOperationCallbackFactory(JobId jobId, int datasetId, int[] primaryKeyFields,
            ITransactionSubsystemProvider txnSubsystemProvider, IndexOperation indexOp, byte resourceType) {
        super(jobId, datasetId, primaryKeyFields, txnSubsystemProvider, resourceType);
        this.indexOp = indexOp;
    }

    @Override
    public IModificationOperationCallback createModificationOperationCallback(long resourceId, Object resource,
            IHyracksTaskContext ctx) throws HyracksDataException {
        ITransactionSubsystem txnSubsystem = txnSubsystemProvider.getTransactionSubsystem(ctx);
        IIndexLifecycleManager indexLifeCycleManager = txnSubsystem.getAsterixAppRuntimeContextProvider()
                .getIndexLifecycleManager();
        ILSMIndex index = (ILSMIndex) indexLifeCycleManager.getIndex(resourceId);
        if (index == null) {
            throw new HyracksDataException("Index(id:" + resourceId + ") is not registered.");
        }

        try {
            ITransactionContext txnCtx = txnSubsystem.getTransactionManager().getTransactionContext(jobId, false);
            IModificationOperationCallback modCallback = new TempDatasetIndexModificationOperationCallback(datasetId,
                    primaryKeyFields, txnCtx, txnSubsystem.getLockManager(), txnSubsystem, resourceId, resourceType,
                    indexOp);
            txnCtx.registerIndexAndCallback(resourceId, index, (AbstractOperationCallback) modCallback, true);
            return modCallback;
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }
}
