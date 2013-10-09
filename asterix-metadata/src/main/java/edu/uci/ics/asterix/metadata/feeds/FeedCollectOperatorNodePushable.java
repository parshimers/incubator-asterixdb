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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FrameReader;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}
 */
public class FeedCollectOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedCollectOperatorNodePushable.class.getName());

    private final int partition;
    private final FeedConnectionId feedConnectionId;
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;
    private final String nodeId;
    private final FrameTupleAccessor fta;
    private final IFeedManager feedManager;
    private final IngestionRuntime ingestionRuntime;

    private CollectionRuntime collectRuntime;
    private FeedFrameWriter feedFrameWriter;
    private FrameReader reader;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedId sourceFeedId,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicy, int partition,
            IngestionRuntime ingestionRuntime) {
        this.partition = partition;
        this.feedConnectionId = feedConnectionId;
        this.ingestionRuntime = ingestionRuntime;
        inbox = new LinkedBlockingQueue<IFeedMessage>();
        this.feedPolicy = feedPolicy;
        policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy);
        nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.COLLECT, feedConnectionId, partition);
            collectRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);

            if (collectRuntime == null) {
                feedFrameWriter = new FeedFrameWriter(writer, this, feedConnectionId, policyEnforcer, nodeId,
                        FeedRuntimeType.COLLECT, partition, fta, feedManager);
                feedFrameWriter.open();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Beginning new feed:" + feedConnectionId);
                }
                collectRuntime = new CollectionRuntime(feedConnectionId, partition, feedFrameWriter, ingestionRuntime);
                feedManager.getFeedConnectionManager().registerFeedRuntime(collectRuntime);
                reader = ingestionRuntime.getFeedWriter().subscribeFeed(feedFrameWriter);
                collectRuntime.setFrameReader(reader);
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Resuming old feed:" + feedConnectionId);
                }
                writer.open();
                FeedFrameWriter frameWriter = (FeedFrameWriter) collectRuntime.getFrameWriter();
                frameWriter.setWriter(frameWriter);
                ((FeedFrameWriter) frameWriter.getWriter()).reset();
                reader = collectRuntime.getReader();
            }

            synchronized (reader) {
                while (!reader.getState().equals(FrameReader.State.FINISHED)) {
                    reader.wait();
                }
            }

            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(collectRuntime.getFeedRuntimeId());
            feedFrameWriter.close();
        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy, switching to INACTIVE INGESTION temporarily");
                }
                feedFrameWriter.fail();
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Interrupted Exception, something went wrong");
                }

                feedManager.getFeedConnectionManager().deRegisterFeedRuntime(collectRuntime.getFeedRuntimeId());
                feedFrameWriter.close();
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }
    
}
