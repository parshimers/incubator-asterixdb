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
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedOperatorInputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.asterix.metadata.feeds.FeedFrameWriter.Mode;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
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
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;
    private final String nodeId;
    private final IFeedManager feedManager;
    private final ISubscribableRuntime sourceRuntime;
    private final IHyracksTaskContext ctx;
    private RecordDescriptor outputRecordDescriptor;
    private FeedOperatorInputSideHandler inputSideHandler;

    private CollectionRuntime collectRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedId sourceFeedId,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicy, int partition,
            ISubscribableRuntime sourceRuntime) {
        this.ctx = ctx;
        this.partition = partition;
        this.feedConnectionId = feedConnectionId;
        this.sourceRuntime = sourceRuntime;
        this.feedPolicy = feedPolicy;
        policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy);
        nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        try {
            outputRecordDescriptor = recordDesc;
            FeedRuntimeType sourceRuntimeType = sourceRuntime.getFeedRuntimeType();
            switch (sourceRuntimeType) {
                case INTAKE:
                    handleCompleteConnection();
                    break;
                case COMPUTE:
                    handlePartialConnection();
                    break;
                default:
                    throw new IllegalStateException("Invalid source type " + sourceRuntimeType);
            }

            collectRuntime.waitTillCollectionOver();
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(collectRuntime.getFeedRuntimeId());
            writer.close();
        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy, switching to " + Mode.STORE
                            + " until failure is resolved");
                }
                inputSideHandler.setMode(FeedOperatorInputSideHandler.Mode.BUFFER_RECOVERY);
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Failure during feed ingestion. Deregistering feed runtime " + collectRuntime
                            + " as feed is not configured to handle failures");
                }
                feedManager.getFeedConnectionManager().deRegisterFeedRuntime(collectRuntime.getFeedRuntimeId());
                writer.close();
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void handlePartialConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(feedConnectionId, FeedRuntimeType.COMPUTE_COLLECT,
                FeedRuntimeId.DEFAULT_OPERAND_ID, partition);
        writer.open();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Beginning new feed (from existing partial connection):" + feedConnectionId);
        }
        IFeedOperatorOutputSideHandler wrapper = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime,
                outputRecordDescriptor, feedConnectionId);

        inputSideHandler = new FeedOperatorInputSideHandler(runtimeId, wrapper, policyEnforcer.getFeedPolicyAccessor(),
                false, ctx.getFrameSize(), new FrameTupleAccessor(ctx.getFrameSize(), recordDesc), recordDesc,
                feedManager);

        collectRuntime = new CollectionRuntime(feedConnectionId, partition, inputSideHandler, sourceRuntime,
                feedPolicy, FeedRuntimeType.COMPUTE_COLLECT);
        feedManager.getFeedConnectionManager().registerFeedRuntime(collectRuntime);
        this.recordDesc = sourceRuntime.getRecordDescriptor();
        sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
    }

    private void handleCompleteConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(feedConnectionId, FeedRuntimeType.COLLECT,
                FeedRuntimeId.DEFAULT_OPERAND_ID, partition);
        collectRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
        if (collectRuntime == null) {
            writer.open();
            IFrameWriter wrapper = writer;
            if (sourceRuntime.getFeedRuntimeType().equals(FeedRuntimeType.COMPUTE)) {
                wrapper = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime, outputRecordDescriptor,
                        feedConnectionId);
                this.recordDesc = sourceRuntime.getRecordDescriptor();
            }

            FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            inputSideHandler = new FeedOperatorInputSideHandler(runtimeId, wrapper,
                    policyEnforcer.getFeedPolicyAccessor(), false, ctx.getFrameSize(), fta, recordDesc, feedManager);

            collectRuntime = new CollectionRuntime(feedConnectionId, partition, inputSideHandler, sourceRuntime,
                    feedPolicy, FeedRuntimeType.COLLECT);
            feedManager.getFeedConnectionManager().registerFeedRuntime(collectRuntime);
            sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Resuming old feed:" + feedConnectionId);
            }
            writer.open();
            FeedFrameCollector frameCollector = collectRuntime.getFrameCollector();
            inputSideHandler = (FeedOperatorInputSideHandler) frameCollector.getFrameWriter();
            IFrameWriter innerWriter = inputSideHandler.getCoreOperator();
            if (innerWriter instanceof CollectTransformFeedFrameWriter) {
                ((CollectTransformFeedFrameWriter) writer).reset(this.writer);
            } else {
                inputSideHandler.reset(this.writer);
            }
            inputSideHandler.setMode(FeedOperatorInputSideHandler.Mode.PROCESS);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reset the internal frame writer for " + collectRuntime + " to "
                        + FeedFrameWriter.Mode.FORWARD + " mode ");
            }
        }
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

}
