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
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector.State;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
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
    private final FeedConnectionId connectionId;
    private final Map<String, String> feedPolicy;
    private final FeedPolicyEnforcer policyEnforcer;
    private final String nodeId;
    private final IFeedManager feedManager;
    private final ISubscribableRuntime sourceRuntime;
    private final IHyracksTaskContext ctx;
    private RecordDescriptor outputRecordDescriptor;
    private FeedRuntimeInputHandler inputSideHandler;

    private CollectionRuntime collectRuntime;

    public FeedCollectOperatorNodePushable(IHyracksTaskContext ctx, FeedId sourceFeedId,
            FeedConnectionId feedConnectionId, Map<String, String> feedPolicy, int partition,
            ISubscribableRuntime sourceRuntime) {
        this.ctx = ctx;
        this.partition = partition;
        this.connectionId = feedConnectionId;
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
            FeedRuntimeType sourceRuntimeType = ((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId())
                    .getFeedRuntimeType();
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

            State state = collectRuntime.waitTillCollectionOver();
            if (state.equals(State.FINISHED)) {
                feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId,
                        collectRuntime.getRuntimeId());
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closing pipeline and moving forward with the Handover stage");
                }
            }
            writer.close();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Closed writer " + writer);
            }
        } catch (InterruptedException ie) {
            handleInterruptedException(ie);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void handleCompleteConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.COLLECT, partition,
                FeedRuntimeId.DEFAULT_OPERAND_ID);
        collectRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(connectionId,
                runtimeId);
        if (collectRuntime == null) {
            beginNewFeed(runtimeId);
        } else {
            resumeOldFeed();
        }
    }

    private void beginNewFeed(FeedRuntimeId runtimeId) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Beginning new feed:" + connectionId);
        }
        writer.open();
        IFrameWriter outputSideWriter = writer;
        if (((SubscribableFeedRuntimeId) sourceRuntime.getRuntimeId()).getFeedRuntimeType().equals(
                FeedRuntimeType.COMPUTE)) {
            outputSideWriter = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime, outputRecordDescriptor,
                    connectionId);
            this.recordDesc = sourceRuntime.getRecordDescriptor();
        }

        inputSideHandler = new FeedRuntimeInputHandler(connectionId, runtimeId, outputSideWriter,
                policyEnforcer.getFeedPolicyAccessor(), false, ctx.getFrameSize(), new FrameTupleAccessor(
                        ctx.getFrameSize(), recordDesc), recordDesc, feedManager);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, outputSideWriter,
                sourceRuntime, feedPolicy);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, collectRuntime);
        sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
    }

    private void resumeOldFeed() throws HyracksDataException {
        writer.open();
        collectRuntime.getFrameCollector().setState(State.HANDOVER); // this will trigger the closing of the downstream operators from the previous pipeline
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Resuming old feed:" + connectionId + " triggered handover");
        }

        this.inputSideHandler = collectRuntime.getInputHandler();
        IFrameWriter innerWriter = inputSideHandler.getCoreOperator();
        if (innerWriter instanceof CollectTransformFeedFrameWriter) {
            ((CollectTransformFeedFrameWriter) writer).reset(this.writer);
        } else {
            inputSideHandler.reset(this.writer);
        }
        inputSideHandler.setMode(Mode.PROCESS);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Reset the internal frame writer for " + collectRuntime + " to " + Mode.PROCESS + " mode");
        }
    }

    private void handlePartialConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.COMPUTE_COLLECT, partition,
                FeedRuntimeId.DEFAULT_OPERAND_ID);
        writer.open();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Beginning new feed (from existing partial connection):" + connectionId);
        }
        IFeedOperatorOutputSideHandler wrapper = new CollectTransformFeedFrameWriter(ctx, writer, sourceRuntime,
                outputRecordDescriptor, connectionId);

        inputSideHandler = new FeedRuntimeInputHandler(connectionId, runtimeId, wrapper,
                policyEnforcer.getFeedPolicyAccessor(), false, ctx.getFrameSize(), new FrameTupleAccessor(
                        ctx.getFrameSize(), recordDesc), recordDesc, feedManager);

        collectRuntime = new CollectionRuntime(connectionId, runtimeId, inputSideHandler, wrapper, sourceRuntime,
                feedPolicy);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, collectRuntime);
        recordDesc = sourceRuntime.getRecordDescriptor();
        sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
    }

    private void handleInterruptedException(InterruptedException ie) throws HyracksDataException {
        if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Continuing on failure as per feed policy, switching to " + Mode.BUFFER_RECOVERY
                        + " until failure is resolved");
            }
            inputSideHandler.setMode(Mode.BUFFER_RECOVERY);
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Failure during feed ingestion. Deregistering feed runtime " + collectRuntime
                        + " as feed is not configured to handle failures");
            }
            feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId, collectRuntime.getRuntimeId());
            writer.close();
            throw new HyracksDataException(ie);
        }
    }

}
