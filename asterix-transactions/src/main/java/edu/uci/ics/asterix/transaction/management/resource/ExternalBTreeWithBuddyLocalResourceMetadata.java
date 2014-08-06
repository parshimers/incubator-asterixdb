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
package edu.uci.ics.asterix.transaction.management.resource;

import java.io.File;
import java.util.Map;

import edu.uci.ics.asterix.common.context.BaseOperationTracker;
import edu.uci.ics.asterix.common.context.DatasetLifecycleManager;
import edu.uci.ics.asterix.common.ioopcallbacks.LSMBTreeWithBuddyIOOperationCallbackFactory;
import edu.uci.ics.asterix.common.transactions.IAsterixAppRuntimeContextProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeUtils;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMMergePolicyFactory;

/**
 * The local resource for disk only lsm btree with buddy tree
 */
public class ExternalBTreeWithBuddyLocalResourceMetadata extends AbstractLSMLocalResourceMetadata {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final ILSMMergePolicyFactory mergePolicyFactory;
    private final Map<String, String> mergePolicyProperties;
    private final int[] buddyBtreeFields;

    public ExternalBTreeWithBuddyLocalResourceMetadata(int datasetID, IBinaryComparatorFactory[] btreeCmpFactories,
            ITypeTraits[] typeTraits, ILSMMergePolicyFactory mergePolicyFactory,
            Map<String, String> mergePolicyProperties, int[] buddyBtreeFields) {
        super(datasetID, null, null, null);
        this.btreeCmpFactories = btreeCmpFactories;
        this.typeTraits = typeTraits;
        this.mergePolicyFactory = mergePolicyFactory;
        this.mergePolicyProperties = mergePolicyProperties;
        this.buddyBtreeFields = buddyBtreeFields;
    }

    @Override
    public ILSMIndex createIndexInstance(IAsterixAppRuntimeContextProvider runtimeContextProvider, String filePath,
            int partition) throws HyracksDataException {
        FileReference file = new FileReference(new File(filePath));
        return LSMBTreeUtils.createExternalBTreeWithBuddy(file, runtimeContextProvider.getBufferCache(),
                runtimeContextProvider.getFileMapManager(), typeTraits, btreeCmpFactories, runtimeContextProvider
                        .getBloomFilterFalsePositiveRate(), mergePolicyFactory.createMergePolicy(mergePolicyProperties,
                        runtimeContextProvider.getIndexLifecycleManager()), new BaseOperationTracker(
                        (DatasetLifecycleManager) runtimeContextProvider.getIndexLifecycleManager(), datasetID),
                runtimeContextProvider.getLSMIOScheduler(), LSMBTreeWithBuddyIOOperationCallbackFactory.INSTANCE
                        .createIOOperationCallback(), buddyBtreeFields, -1);
    }
}
