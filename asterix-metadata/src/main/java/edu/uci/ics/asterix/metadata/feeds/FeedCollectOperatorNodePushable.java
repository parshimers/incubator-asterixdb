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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedOperatorInputSideHandler;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;
import edu.uci.ics.asterix.metadata.feeds.FeedFrameWriter.Mode;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
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

    private CollectionRuntime collectRuntime;
    private FeedFrameWriter feedFrameWriter;

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
            feedFrameWriter.close();
        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy, switching to " + Mode.STORE
                            + " until failure is resolved");
                }
                feedFrameWriter.setMode(Mode.STORE);
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Failure during feed ingestion. Deregistering feed runtime " + collectRuntime
                            + " as feed is not configured to handle failures");
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

    private void handlePartialConnection() throws Exception {
        feedFrameWriter = new FeedFrameWriter(ctx, writer, this, feedConnectionId, policyEnforcer, nodeId,
                FeedRuntimeType.COMPUTE_COLLECT, partition, outputRecordDescriptor, feedManager);
        feedFrameWriter.open();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Beginning new feed (from existing partial connection):" + feedConnectionId);
        }
        IFeedOperatorOutputSideHandler wrapper = feedFrameWriter;
        wrapper = new CollectTransformFeedFrameWriter(ctx, feedFrameWriter, sourceRuntime, outputRecordDescriptor,
                feedConnectionId);
        collectRuntime = new CollectionRuntime(feedConnectionId, partition, wrapper, sourceRuntime, feedPolicy,
                FeedRuntimeType.COMPUTE_COLLECT);
        feedManager.getFeedConnectionManager().registerFeedRuntime(collectRuntime);
        this.recordDesc = sourceRuntime.getRecordDescriptor();
        sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
    }

    private void handleCompleteConnection() throws Exception {
        FeedRuntimeId runtimeId = new FeedRuntimeId(feedConnectionId, FeedRuntimeType.COLLECT,
                FeedRuntimeId.DEFAULT_OPERAND_ID, partition);
        collectRuntime = (CollectionRuntime) feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
        if (collectRuntime == null) {
            feedFrameWriter = new FeedFrameWriter(ctx, writer, this, feedConnectionId, policyEnforcer, nodeId,
                    FeedRuntimeType.COLLECT, partition, outputRecordDescriptor, feedManager);
            feedFrameWriter.open();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Beginning new feed:" + feedConnectionId);
            }

            IFeedOperatorOutputSideHandler wrapper = feedFrameWriter;
            if (sourceRuntime.getFeedRuntimeType().equals(FeedRuntimeType.COMPUTE)) {
                wrapper = new CollectTransformFeedFrameWriter(ctx, feedFrameWriter, sourceRuntime,
                        outputRecordDescriptor, feedConnectionId);
            }

            collectRuntime = new CollectionRuntime(feedConnectionId, partition, wrapper, sourceRuntime, feedPolicy,
                    FeedRuntimeType.COLLECT);
            feedManager.getFeedConnectionManager().registerFeedRuntime(collectRuntime);
            if (sourceRuntime.getFeedRuntimeType().equals(FeedRuntimeType.COMPUTE)) {
                this.recordDesc = sourceRuntime.getRecordDescriptor();
            }
            sourceRuntime.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(), collectRuntime);
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Resuming old feed:" + feedConnectionId);
            }
            writer.open();
            FeedFrameCollector frameReader = collectRuntime.getFrameCollector();
            feedFrameWriter = (FeedFrameWriter) frameReader.getFrameWriter();
            feedFrameWriter.setWriter(writer);
            //feedFrameWriter.getWriter()).reset();
            feedFrameWriter.setMode(FeedFrameWriter.Mode.FORWARD);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reset the internal frame writer for " + collectRuntime + " to "
                        + FeedFrameWriter.Mode.FORWARD + " mode ");
            }
        }
    }

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public static class CollectTransformFeedFrameWriter implements IFeedOperatorOutputSideHandler {

        private final FeedConnectionId connectionId;
        private final IFeedOperatorOutputSideHandler downstreamWriter;
        private final FrameTupleAccessor inputFrameTupleAccessor;
        private final FrameTupleAppender tupleAppender;
        private final ByteBuffer frame;

        private ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);

        public CollectTransformFeedFrameWriter(IHyracksTaskContext ctx, IFeedOperatorOutputSideHandler downstreamWriter,
                ISubscribableRuntime sourceRuntime, RecordDescriptor outputRecordDescriptor,
                FeedConnectionId connectionId) throws HyracksDataException {
            this.downstreamWriter = downstreamWriter;
            RecordDescriptor inputRecordDescriptor = sourceRuntime.getRecordDescriptor();
            inputFrameTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecordDescriptor);
            tupleAppender = new FrameTupleAppender(ctx.getFrameSize());
            frame = ctx.allocateFrame();
            tupleAppender.reset(frame, true);
            this.connectionId = connectionId;
        }

        @Override
        public void open() throws HyracksDataException {
            downstreamWriter.open();
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            inputFrameTupleAccessor.reset(buffer);
            int nTuple = inputFrameTupleAccessor.getTupleCount();
            for (int t = 0; t < nTuple; t++) {
                tupleBuilder.addField(inputFrameTupleAccessor, t, 0);
                appendTupleToFrame();
                tupleBuilder.reset();
            }
        }

        private void appendTupleToFrame() throws HyracksDataException {
            if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                FrameUtils.flushFrame(frame, downstreamWriter);
                tupleAppender.reset(frame, true);
                if (!tupleAppender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize())) {
                    throw new IllegalStateException();
                }
            }
        }

        @Override
        public void fail() throws HyracksDataException {
            downstreamWriter.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            downstreamWriter.close();
        }

        @Override
        public FeedId getFeedId() {
            return downstreamWriter.getFeedId();
        }

        @Override
        public Type getType() {
            return Type.COLLECT_TRANSFORM_FEED_OUTPUT_HANDLER;
        }

        public IFeedOperatorOutputSideHandler getDownstreamWriter() {
            return downstreamWriter;
        }

        public FeedConnectionId getConnectionId() {
            return connectionId;
        }

    }

}
