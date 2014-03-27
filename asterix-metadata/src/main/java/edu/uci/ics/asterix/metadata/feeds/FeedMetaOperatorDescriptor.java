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
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeState;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedFrameProcessor;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.FrameDistributor;
import edu.uci.ics.asterix.common.feeds.IFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.SubscribableRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

/**
 * FeedMetaOperatorDescriptor is a wrapper operator that provides a sanboox like
 * environment for an hyracks operator that is part of a feed ingestion
 * pipeline. The MetaFeed operator provides an interface iden- tical to that
 * offered by the underlying wrapped operator, hereafter referred to as the core
 * operator. As seen by Hyracks, the altered pipeline is identical to the
 * earlier version formed from core operators. The MetaFeed operator enhances
 * each core operator by providing functionality for handling runtime
 * exceptions, saving any state for future retrieval, and measuring/reporting of
 * performance characteristics. We next describe how the added functionality
 * contributes to providing fault- tolerance.
 */

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedMetaOperatorDescriptor.class.getName());

    /**
     * The actual (Hyracks) operator that is wrapped around by the Metafeed
     * Adaptor
     **/
    private IOperatorDescriptor coreOperator;

    /**
     * A unique identifier for the feed instance. A feed instance represents the
     * flow of data from a feed to a dataset.
     **/
    private final FeedConnectionId feedConnectionId;

    /**
     * The policy associated with the feed instance.
     */
    private final Map<String, String> feedPolicyProperties;

    /*
     * type for the feed runtime associated with the operator.
     * Possible values: INTAKE, COMPUTE, STORAGE, COMMIT
     */
    private final FeedRuntimeType runtimeType;

    /**
     * true indicates that the runtime can be subscribed for data by other
     * runtime instances.
     **/
    private final boolean enableSubscriptionMode;

    private final String operandId;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, Map<String, String> feedPolicyProperties,
            FeedRuntimeType runtimeType, boolean enableSubscriptionMode, String operandId) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicyProperties = feedPolicyProperties;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
        this.enableSubscriptionMode = enableSubscriptionMode;
        this.operandId = operandId;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                feedConnectionId, feedPolicyProperties, runtimeType, enableSubscriptionMode, operandId);
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        /** Runtime node pushable corresponding to the core feed operator **/
        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;

        /**
         * A policy enforcer that ensures dyanmic decisions for a feed are taken
         * in accordance with the associated ingestion policy
         **/
        private FeedPolicyEnforcer policyEnforcer;

        /**
         * The Feed Runtime instance associated with the operator. Feed Runtime
         * captures the state of the operator while the feed is active.
         */
        private IFeedRuntime feedRuntime;

        /**
         * A unique identifier for the feed instance. A feed instance represents
         * the flow of data from a feed to a dataset.
         **/
        private FeedConnectionId feedConnectionId;

        /**
         * Denotes the i'th operator instance in a setting where K operator
         * instances are scheduled to run in parallel
         **/
        private int partition;

        /**
         * A buffer that is used to hold the current frame that is being
         * processed
         **/
        private ByteBuffer currentBuffer;

        /** Type associated with the core feed operator **/
        private final FeedRuntimeType runtimeType;

        /** True is the feed is recovering from a previous failed execution **/
        private boolean resumeOldState;

        /** The Node Controller ID for the host NC **/

        private String nodeId;

        /** The (singleton) instance of IFeedManager **/
        private IFeedManager feedManager;

        /**
         * The frame reader instance that reads the input frames from the
         * distribute feed writer
         **/
        private FeedFrameCollector outputSideFrameCollector;

        /**
         * true indicates that the runtime can be subscribed for data by other
         * runtime instances.
         **/
        private final boolean enableSubscriptionMode;

        private final boolean inputSideBufferring;

        private final FrameDistributor frameDistributor;

        /**
         * The frame reader instance that reads the input frames from upstream
         * operator
         **/
        private FeedFrameCollector inputSideFrameCollector;

        private boolean recoverSoftFailure;

        private FrameTupleAccessor fta;

        private final IHyracksTaskContext ctx;

        private final String operandId;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                Map<String, String> feedPolicyProperties, FeedRuntimeType runtimeType, boolean enableSubscriptionMode,
                String operationId) throws HyracksDataException {
            this.ctx = ctx;
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
            this.partition = partition;
            this.runtimeType = runtimeType;
            this.feedConnectionId = feedConnectionId;
            this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
            this.feedManager = ((IAsterixAppRuntimeContext) (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject()).getFeedManager();
            this.enableSubscriptionMode = enableSubscriptionMode; // set to true
                                                                  // for
                                                                  // COMPUTE
                                                                  // operator
                                                                  // when feed
                                                                  // has an
                                                                  // associated
                                                                  // UDF.
            this.inputSideBufferring = runtimeType.equals(FeedRuntimeType.COMPUTE); // &&
                                                                                    // enableSubscriptionMode;
            if (inputSideBufferring) {
                frameDistributor = new FrameDistributor(feedConnectionId.getFeedId(), runtimeType, partition, false,
                        feedManager.getFeedMemoryManager(), ctx.getFrameSize());
            } else {
                frameDistributor = null;
            }
            recoverSoftFailure = policyEnforcer.getFeedPolicyAccessor().continueOnSoftFailure();
            fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject();
            this.feedManager = runtimeCtx.getFeedManager();
            this.operandId = operationId;
        }

        @Override
        public void open() throws HyracksDataException {
            FeedRuntimeId runtimeId = new FeedRuntimeId(feedConnectionId, runtimeType, operandId, partition);
            try {
                feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                IFeedFrameWriter mWriter = new FeedFrameWriter(ctx, writer, this, feedConnectionId, policyEnforcer,
                        nodeId, runtimeType, partition, recordDesc, feedManager);
                if (frameDistributor != null) {
                    frameDistributor.setFta(new FrameTupleAccessor(ctx.getFrameSize(), recordDesc));
                }
                if (feedRuntime == null) {
                    switch (runtimeType) {
                        case COMPUTE:
                            wrapComputeOperator(mWriter);
                            break;
                        case COMMIT:
                        case STORE:
                            registerBasicFeedRuntime(mWriter);
                            break;
                        case COLLECT:
                        case COMPUTE_COLLECT:
                        case INTAKE:
                            throw new IllegalStateException("Invalid wrapping of " + runtimeType
                                    + " by meta feed operator");
                    }

                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Did not find a saved state from a previous zombie, starting a new instance for "
                                + runtimeType + " node.");
                    }
                    resumeOldState = false;
                } else {
                    switch (runtimeType) {
                        case COMPUTE:
                            if (enableSubscriptionMode) {
                                retrieveSubscribableFeedRuntime(feedRuntime, mWriter);
                            } else {
                                coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
                            }
                            break;
                        case COMMIT:
                        case STORE:
                            coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
                            break;

                    }
                    resumeOldState = true;
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Retreived state from the zombie instance from previous execution for "
                                + runtimeType + " node.");
                    }
                }
                coreOperatorNodePushable.open();
                fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to initialize feed operator " + runtimeType + " [" + partition + "]");
                }
                throw new HyracksDataException(e);
            }
        }

        private void wrapComputeOperator(IFeedFrameWriter writer) throws Exception {
            IFeedFrameWriter outputWriter = null;
            if (enableSubscriptionMode) {
                outputWriter = registerSubscribableRuntime(writer); // DistributeFeedFrameWriter
            } else {
                outputWriter = writer;
                registerBasicFeedRuntime(writer);
            }
            if (inputSideBufferring) {
                inputSideFrameCollector = new FeedFrameCollector(frameDistributor,
                        policyEnforcer.getFeedPolicyAccessor(), coreOperatorNodePushable, feedConnectionId.getFeedId());
                frameDistributor.registerFrameCollector(inputSideFrameCollector);
            }
        }

        private void retrieveSubscribableFeedRuntime(IFeedRuntime feedRuntime, IFeedFrameWriter frameWriter) {
            DistributeFeedFrameWriter dWriter = (DistributeFeedFrameWriter) feedRuntime.getFeedFrameWriter();
            dWriter.setWriter(writer);
            Map<IFrameWriter, FeedFrameCollector> registeredReaders = dWriter.getRegisteredReaders();
            for (Entry<IFrameWriter, FeedFrameCollector> entry : registeredReaders.entrySet()) {
                if (entry.getValue().equals(outputSideFrameCollector)) {
                    outputSideFrameCollector.setFrameWriter(frameWriter);
                    break;
                }
            }
            coreOperatorNodePushable.setOutputFrameWriter(0, dWriter, recordDesc);
        }

        private void registerBasicFeedRuntime(IFeedFrameWriter mWriter) throws Exception {
            feedRuntime = new BasicFeedRuntime(feedConnectionId, partition, mWriter, runtimeType);
            feedManager.getFeedConnectionManager().registerFeedRuntime((BasicFeedRuntime) feedRuntime);
            coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered basic feed runtime " + feedRuntime);
            }
        }

        private IFeedFrameWriter registerSubscribableRuntime(IFeedFrameWriter feedFrameWriter) throws Exception {
            FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(feedConnectionId.getFeedId(),
                    writer, runtimeType, partition, fta, feedManager, ctx.getFrameSize());
            outputSideFrameCollector = distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(),
                    feedFrameWriter);
            FeedSubscribableRuntimeId sid = new FeedSubscribableRuntimeId(feedConnectionId.getFeedId(), runtimeType,
                    partition);
            feedRuntime = new SubscribableRuntime(sid, distributeWriter, runtimeType, recordDesc);
            feedManager.getFeedSubscriptionManager()
                    .registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
            coreOperatorNodePushable.setOutputFrameWriter(0, distributeWriter, recordDesc);
            return distributeWriter;
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (resumeOldState) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("State from previous zombie instance ");
                    }
                    resumeOldState = false;
                }
                if (inputSideBufferring) { // buffering
                    frameDistributor.nextFrame(buffer);
                } else { // no buffering
                    currentBuffer = buffer;
                    FeedFrameProcessor.nextFrame(feedConnectionId, coreOperatorNodePushable, buffer,
                            recoverSoftFailure, ctx.getFrameSize(), fta, recordDesc, feedManager);
                    //coreOperatorNodePushable.nextFrame(buffer);
                    currentBuffer = null;
                }
            } catch (Exception e) {
                throw e;
            }

        }

        @Override
        public void fail() throws HyracksDataException {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " fail ");
            }
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (currentBuffer != null) {
                    FeedRuntimeState runtimeState = new FeedRuntimeState(currentBuffer, writer, null);
                    // feedRuntime.setFeedRuntimeState(runtimeState);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Saved feed compute runtime for revivals" + feedRuntime.getFeedId());
                    }
                } else {
                    // feedManager.getFeedConnectionManager().deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.warning("No state to save, de-registered feed runtime " + feedRuntime.getFeedId());
                    }
                }
            }
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            if (frameDistributor != null) {
                frameDistributor.close();
            }
            coreOperatorNodePushable.close();
            switch (feedRuntime.getFeedRuntimeType()) {
                case STORE:
                case COMMIT:
                case COLLECT:
                    feedManager.getFeedConnectionManager().deRegisterFeedRuntime(
                            ((BasicFeedRuntime) feedRuntime).getFeedRuntimeId());
                    break;
                case COMPUTE:
                    if (enableSubscriptionMode) {
                        FeedSubscribableRuntimeId runtimeId = ((ISubscribableRuntime) feedRuntime)
                                .getFeedSubscribableRuntimeId();
                        feedManager.getFeedSubscriptionManager().deregisterFeedSubscribableRuntime(runtimeId);
                    } else {
                        feedManager.getFeedConnectionManager().deRegisterFeedRuntime(
                                ((BasicFeedRuntime) feedRuntime).getFeedRuntimeId());
                    }
                    break;
            }
        }

    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

}
