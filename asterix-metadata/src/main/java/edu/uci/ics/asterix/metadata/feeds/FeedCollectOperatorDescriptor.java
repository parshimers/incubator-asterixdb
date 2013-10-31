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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedIngestionId;
import edu.uci.ics.asterix.common.feeds.IFeedConnectionManager;
import edu.uci.ics.asterix.common.feeds.IFeedIngestionManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.asterix.metadata.feeds.FeedSubscriptionRequest.SubscriptionLocation;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

/**
 * FeedCollectOperatorDescriptor is responsible for ingesting data from an external source. This
 * operator uses a user specified for a built-in adaptor for retrieving data from the external
 * data source.
 */
public class FeedCollectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(FeedCollectOperatorDescriptor.class.getName());

    /** The type associated with the ADM data output from the feed adaptor */
    private final IAType outptuType;

    /** unique identifier for a feed instance. */
    private final FeedConnectionId feedConnectionId;

    /** Map representation of policy parameters */
    private final Map<String, String> feedPolicy;

    /** The (singleton) instance of {@code IFeedIngestionManager} **/
    private IFeedIngestionManager feedIngestionManager;

    /** The source feed from which the feed derives its data from. **/
    private final FeedId sourceFeedId;

    /** The subscription location at which the recipient feed receives tuples from the source feed **/
    private final SubscriptionLocation subscriptionLocation;

    public FeedCollectOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId, FeedId sourceFeedId,
            ARecordType atype, RecordDescriptor rDesc, Map<String, String> feedPolicy,
            SubscriptionLocation subscriptionLocation) {
        super(spec, 0, 1);
        recordDescriptors[0] = rDesc;
        this.outptuType = atype;
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        this.sourceFeedId = sourceFeedId;
        this.subscriptionLocation = subscriptionLocation;
    }

    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedIngestionManager = runtimeCtx.getFeedManager().getFeedIngestionManager();
        switch (subscriptionLocation) {
            case SOURCE_FEED_INTAKE:
                IngestionRuntime ingestionRuntime = null;
                try {
                    FeedIngestionId feedIngestionId = new FeedIngestionId(sourceFeedId, partition);
                    ingestionRuntime = getIngestionRuntime(feedIngestionId);
                    if (ingestionRuntime == null) {
                        throw new HyracksDataException("Source ingestion task not found for source feed id "
                                + sourceFeedId);
                    }

                } catch (Exception exception) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Initialization of the feed adaptor failed with exception " + exception);
                    }
                    throw new HyracksDataException("Initialization of the feed adapter failed", exception);
                }
                break;
            case SOURCE_FEED_COMPUTE:
                ingestionRuntime = feedIngestionManager.getIngestionRuntime(feedIngestionId);
                break;
        }
        return new FeedCollectOperatorNodePushable(ctx, sourceFeedId, feedConnectionId, feedPolicy, partition,
                ingestionRuntime);
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public IAType getOutputType() {
        return outptuType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptors[0];
    }

    public FeedId getSourceFeedId() {
        return sourceFeedId;
    }

    private IngestionRuntime getIngestionRuntime(FeedIngestionId feedIngestionId) {
        int waitCycleCount = 0;
        IngestionRuntime ingestionRuntime = feedIngestionManager.getIngestionRuntime(feedIngestionId);
        while (ingestionRuntime == null && waitCycleCount < 5) {
            try {
                Thread.sleep(2000);
                waitCycleCount++;
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            ingestionRuntime = feedIngestionManager.getIngestionRuntime(feedIngestionId);
        }
        return ingestionRuntime;
    }
}
