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
package edu.uci.ics.asterix.external.indexing.operators;

import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.util.IndexFileNameUtil;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ITwoPCIndex;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;

public class ExternalDatasetIndexesCommitOperatorDescriptor extends AbstractExternalDatasetIndexesOperatorDescriptor {

    public ExternalDatasetIndexesCommitOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory,
            IndexInfoOperatorDescriptor fileIndexesInfo,
            List<ExternalBTreeWithBuddyDataflowHelperFactory> bTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> bTreeIndexesInfos,
            List<ExternalRTreeDataflowHelperFactory> rTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> rTreeIndexesInfos) {
        super(spec, filesIndexDataflowHelperFactory, fileIndexesInfo, bTreeIndexesDataflowHelperFactories,
                bTreeIndexesInfos, rTreeIndexesDataflowHelperFactories, rTreeIndexesInfos);
    }

    private static final long serialVersionUID = 1L;

    @Override
    protected void performOpOnIndex(IIndexDataflowHelperFactory indexDataflowHelperFactory, IHyracksTaskContext ctx,
            IndexInfoOperatorDescriptor fileIndexInfo, int partition) throws Exception {
        System.err.println("performing the operation on "+ IndexFileNameUtil.prepareFileName(fileIndexInfo.getFileSplitProvider()
                .getFileSplits()[partition].getLocalFile().getFile().getPath(), fileIndexInfo.getFileSplitProvider()
                .getFileSplits()[partition].getIODeviceId()));
        // Get DataflowHelper
        IIndexDataflowHelper indexHelper = indexDataflowHelperFactory.createIndexDataflowHelper(fileIndexInfo, ctx, partition);
        // Get index
        IIndex index = indexHelper.getIndexInstance();
        // commit transaction
        ((ITwoPCIndex) index).commitTransaction();
        System.err.println("operation on "+ IndexFileNameUtil.prepareFileName(fileIndexInfo.getFileSplitProvider()
                .getFileSplits()[partition].getLocalFile().getFile().getPath(), fileIndexInfo.getFileSplitProvider()
                .getFileSplits()[partition].getIODeviceId()) + " Succeded");

    }

}
