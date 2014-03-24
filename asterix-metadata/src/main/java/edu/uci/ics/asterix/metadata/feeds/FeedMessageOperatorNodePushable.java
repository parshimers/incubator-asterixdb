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
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector.Mode;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorNodePushable.CollectTransformFeedFrameWriter;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * Runtime for the @see{FeedMessageOperatorDescriptor}
 */
public class FeedMessageOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageOperatorNodePushable.class.getName());

    private final FeedConnectionId feedConnectionId;
    private final IFeedMessage feedMessage;
    private final int partition;
    private final IHyracksTaskContext ctx;
    private final IFeedManager feedManager;

    public FeedMessageOperatorNodePushable(IHyracksTaskContext ctx, FeedConnectionId feedConnectionId,
            IFeedMessage feedMessage, int partition, int nPartitions) {
        this.feedConnectionId = feedConnectionId;
        this.feedMessage = feedMessage;
        this.partition = partition;
        this.ctx = ctx;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            writer.open();

            switch (feedMessage.getMessageType()) {
                case END:
                    EndFeedMessage endFeedMessage = (EndFeedMessage) feedMessage;

                    switch (endFeedMessage.getEndMessageType()) {
                        case DISCONNECT_FEED:
                            hanldeDisconnectFeedTypeMessage(endFeedMessage);
                            break;
                        case DISCONTINUE_SOURCE:
                            handleDiscontinueFeedTypeMessage(endFeedMessage);
                            break;
                    }

                    break;

            }

        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    private void handleDiscontinueFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        FeedId sourceFeedId = endFeedMessage.getSourceFeedId();
        FeedSubscribableRuntimeId subscribableRuntimeId = new FeedSubscribableRuntimeId(sourceFeedId,
                FeedRuntimeType.INTAKE, partition);
        ISubscribableRuntime feedRuntime = feedManager.getFeedSubscriptionManager().getSubscribableRuntime(
                subscribableRuntimeId);
        IAdapterRuntimeManager adapterRuntimeManager = ((IngestionRuntime) feedRuntime).getAdapterRuntimeManager();
        adapterRuntimeManager.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopped Adapter " + adapterRuntimeManager);
        }
    }

    private void hanldeDisconnectFeedTypeMessage(EndFeedMessage endFeedMessage) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Ending feed:" + endFeedMessage.getFeedConnectionId());
        }
        FeedRuntimeId runtimeId = null;
        FeedRuntimeType subscribaleRuntimeType = ((EndFeedMessage) feedMessage).getSourceRuntimeType();
        if (endFeedMessage.isCompleteDisconnection()) {
            // subscribableRuntimeType represents the location at which it receives data
            switch (subscribaleRuntimeType) {
                case INTAKE:
                case COMPUTE:
                    BasicFeedRuntime feedRuntime = null;
                    runtimeId = new FeedRuntimeId(FeedRuntimeType.COMPUTE_COLLECT, feedConnectionId, partition);
                    feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                    if (feedRuntime == null) {
                        runtimeId = new FeedRuntimeId(FeedRuntimeType.COLLECT, feedConnectionId, partition);
                        feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                    }
                    feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                    ((CollectionRuntime) feedRuntime).getSourceRuntime().unsubscribeFeed(
                            (CollectionRuntime) feedRuntime);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("COMPLETE UNSUBSCRIPTION of " + endFeedMessage.getFeedConnectionId());
                    }
                    break;
            }
        } else {
            // subscribaleRuntimeType represents the location for data hand-off in presence of subscribers
            switch (subscribaleRuntimeType) {
                case INTAKE:
                    // illegal state as data hand-off from one feed to another does not happen at ingest
                    throw new IllegalStateException("Illegal State, invalid runtime type  " + subscribaleRuntimeType);
                case COMPUTE:
                    // feed could be primary or secondary, doesn't matter
                    FeedSubscribableRuntimeId feedSubscribableRuntimeId = new FeedSubscribableRuntimeId(
                            feedConnectionId.getFeedId(), FeedRuntimeType.COMPUTE, partition);
                    ISubscribableRuntime feedRuntime = feedManager.getFeedSubscriptionManager().getSubscribableRuntime(
                            feedSubscribableRuntimeId);
                    DistributeFeedFrameWriter dWriter = (DistributeFeedFrameWriter) feedRuntime.getFeedFrameWriter();
                    Map<IFrameWriter, FeedFrameCollector> registeredCollectors = dWriter.getRegisteredReaders();
                    IFeedFrameWriter unsubscribingWriter = null;
                    for (Entry<IFrameWriter, FeedFrameCollector> entry : registeredCollectors.entrySet()) {
                        IFrameWriter frameWriter = entry.getKey();
                        FeedFrameCollector collector = entry.getValue();
                        if (collector.getMode().equals(Mode.FORWARD_TO_WRITER)) {
                            IFeedFrameWriter feedFrameWriter = (IFeedFrameWriter) frameWriter;
                            FeedConnectionId connectionId = null;
                            switch (feedFrameWriter.getType()) {
                                case BASIC_FEED_WRITER:
                                    connectionId = ((FeedFrameWriter) feedFrameWriter).getFeedConnectionId();
                                    break;
                                case COLLECT_TRANSFORM_FEED_WRITER:
                                    connectionId = ((FeedFrameWriter) ((CollectTransformFeedFrameWriter) frameWriter)
                                            .getDownstreamWriter()).getFeedConnectionId();

                                    break;
                                case DISTRIBUTE_FEED_WRITER:
                                    throw new IllegalStateException("Invalid feed frame writer type "
                                            + feedFrameWriter.getType());
                            }
                            if (connectionId.equals(endFeedMessage.getFeedConnectionId())) {
                                unsubscribingWriter = feedFrameWriter;
                                break;
                            }
                        }
                    }
                    if (unsubscribingWriter != null) {
                        dWriter.unsubscribeFeed(unsubscribingWriter);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("PARTIAL UNSUBSCRIPTION of " + unsubscribingWriter);
                        }
                    } else {
                        throw new HyracksDataException("Unable to unsubscribe!!!! " + feedConnectionId);
                    }
                    break;
            }

        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed from feed :" + feedConnectionId);
        }
    }
}
