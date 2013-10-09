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
package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedIngestionId;
import edu.uci.ics.asterix.common.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.common.feeds.IFeedAdapter;
import edu.uci.ics.asterix.common.feeds.IFeedIngestionManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * Sends a control message to the registered message queue for feed specified by its feedId.
 */
public class FeedIntakeOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final FeedId feedId;

    private final IAdapterFactory adapterFactory;

    public FeedIntakeOperatorDescriptor(JobSpecification spec, String dataverse, String feedName,
            IAdapterFactory adapterFactory) {
        super(spec, 0, 1);
        this.feedId = new FeedId(dataverse, feedName);
        this.adapterFactory = adapterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        IDatasourceAdapter adapter;
        try {
            adapter = adapterFactory.createAdapter(ctx, partition);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        IFeedIngestionManager feedIngestionManager = runtimeCtx.getFeedManager().getFeedIngestionManager();
        FeedIngestionId feedIngestionId = new FeedIngestionId(feedId, partition);

        IngestionRuntime ingestionRuntime = (IngestionRuntime) feedIngestionManager
                .getIngestionRuntime(feedIngestionId);
        return new FeedIntakeOperatorNodePushable(ctx, feedId, (IFeedAdapter) adapter, partition, ingestionRuntime);
    }

}
