package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeState;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.IFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.SubscribableRuntime;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
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
import edu.uci.ics.hyracks.storage.am.common.api.TreeIndexException;

/**
 * FeedMetaOperatorDescriptor is a wrapper operator that provides a sanboox like
 * environment for an hyracks operator that is part of a feed ingestion pipeline.
 * The MetaFeed operator provides an interface iden- tical to that offered by the
 * underlying wrapped operator, hereafter referred to as the core operator.
 * As seen by Hyracks, the altered pipeline is identical to the earlier version formed
 * from core operators. The MetaFeed operator enhances each core operator by providing
 * functionality for handling runtime exceptions, saving any state for future retrieval,
 * and measuring/reporting of performance characteristics. We next describe how the added
 * functionality contributes to providing fault- tolerance.
 */

public class FeedMetaOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedMetaOperatorDescriptor.class.getName());

    /** The actual (Hyracks) operator that is wrapped around by the Metafeed Adaptor **/
    private IOperatorDescriptor coreOperator;

    /**
     * A unique identifier for the feed instance. A feed instance represents the flow of data
     * from a feed to a dataset.
     **/
    private final FeedConnectionId feedConnectionId;

    /**
     * The policy associated with the feed instance.
     */
    private final FeedPolicy feedPolicy;

    /**
     * type for the feed runtime associated with the operator.
     * Possible values: INGESTION, COMPUTE, STORAGE, COMMIT
     */
    private final FeedRuntimeType runtimeType;

    /** true indicates that the runtime can be subscribed for data by other runtime instances. **/
    private final boolean enableSubscriptionMode;

    public FeedMetaOperatorDescriptor(JobSpecification spec, FeedConnectionId feedConnectionId,
            IOperatorDescriptor coreOperatorDescriptor, FeedPolicy feedPolicy, FeedRuntimeType runtimeType,
            boolean enableSubscriptionMode) {
        super(spec, coreOperatorDescriptor.getInputArity(), coreOperatorDescriptor.getOutputArity());
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        if (coreOperatorDescriptor.getOutputRecordDescriptors().length == 1) {
            recordDescriptors[0] = coreOperatorDescriptor.getOutputRecordDescriptors()[0];
        }
        this.coreOperator = coreOperatorDescriptor;
        this.runtimeType = runtimeType;
        this.enableSubscriptionMode = enableSubscriptionMode;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        return new FeedMetaNodePushable(ctx, recordDescProvider, partition, nPartitions, coreOperator,
                feedConnectionId, feedPolicy, runtimeType, enableSubscriptionMode);
    }

    @Override
    public String toString() {
        return "FeedMeta [" + coreOperator + " ]";
    }

    private static class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

        /** Runtime node pushable corresponding to the core feed operator **/
        private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperatorNodePushable;

        /**
         * A policy enforcer that ensures dyanmic decisions for a feed are taken in accordance
         * with the associated ingestion policy
         **/
        private FeedPolicyEnforcer policyEnforcer;

        /**
         * The Feed Runtime instance associated with the operator. Feed Runtime captures the state of the operator while
         * the feed is active.
         */
        private IFeedRuntime feedRuntime;

        /**
         * A unique identifier for the feed instance. A feed instance represents the flow of data
         * from a feed to a dataset.
         **/
        private FeedConnectionId feedConnectionId;

        /** Denotes the i'th operator instance in a setting where K operator instances are scheduled to run in parallel **/
        private int partition;

        /** A buffer that is used to hold the current frame that is being processed **/
        private ByteBuffer currentBuffer;

        /** Type associated with the core feed operator **/
        private final FeedRuntimeType runtimeType;

        /** True is the feed is recovering from a previous failed execution **/
        private boolean resumeOldState;

        /** The Node Controller ID for the host NC **/

        private String nodeId;

        /** Allows to iterate over the tuples in a frame **/
        private FrameTupleAccessor fta;

        /** The (singleton) instance of IFeedManager **/
        private IFeedManager feedManager;

        /** true indicates that the runtime can be subscribed for data by other runtime instances. **/
        private final boolean enableSubscriptionMode;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                FeedPolicy feedPolicy, FeedRuntimeType runtimeType, boolean enableSubscriptionMode)
                throws HyracksDataException {
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy.getProperties());
            this.partition = partition;
            this.runtimeType = runtimeType;
            this.feedConnectionId = feedConnectionId;
            this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
            this.fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            this.feedManager = ((IAsterixAppRuntimeContext) (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject()).getFeedManager();
            this.enableSubscriptionMode = enableSubscriptionMode;

        }

        @Override
        public void open() throws HyracksDataException {
            FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, feedConnectionId, partition);
            try {
                feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                if (feedRuntime == null) {
                    switch (runtimeType) {
                        case COMPUTE:
                            if (enableSubscriptionMode) {
                                registerSubscribableRuntime();
                            } else {
                                registerBasicFeedRuntime();
                            }
                            break;
                        case COMMIT:
                        case STORE:
                            registerBasicFeedRuntime();
                            break;
                        case COLLECT:
                        case INGEST:
                            throw new IllegalStateException("Invalid wrapping of " + runtimeType
                                    + " by meta feed operator");
                    }

                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Did not find a saved state from a previous zombie, starting a new instance for "
                                + runtimeType + " node.");
                    }
                    resumeOldState = false;
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Retreived state from the zombie instance from previous execution for "
                                + runtimeType + " node.");
                    }
                    resumeOldState = true;
                }
                coreOperatorNodePushable.open();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to initialize feed operator " + feedRuntime + " [" + partition + "]");
                }
                throw new HyracksDataException(e);
            }
        }

        private void registerBasicFeedRuntime() throws Exception {
            feedRuntime = new BasicFeedRuntime(feedConnectionId, partition, runtimeType);
            IFeedFrameWriter mWriter = new FeedFrameWriter(writer, this, feedConnectionId, policyEnforcer, nodeId,
                    runtimeType, partition, fta, feedManager);
            feedManager.getFeedConnectionManager().registerFeedRuntime((BasicFeedRuntime) feedRuntime);
            coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered basic feed runtime " + feedRuntime);
            }
        }

        private void registerSubscribableRuntime() throws Exception {
            IFeedFrameWriter feedFrameWriter = new FeedFrameWriter(writer, this, feedConnectionId, policyEnforcer,
                    nodeId, runtimeType, partition, fta, feedManager);
            IFeedFrameWriter mWriter = new DistributeFeedFrameWriter(feedConnectionId.getFeedId(), writer);
            ((DistributeFeedFrameWriter) mWriter).subscribeFeed(feedFrameWriter);
            FeedSubscribableRuntimeId sid = new FeedSubscribableRuntimeId(feedConnectionId.getFeedId(), partition);
            feedRuntime = new SubscribableRuntime(sid, (DistributeFeedFrameWriter) mWriter, runtimeType);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Set up distrbute feed frame writer for " + FeedRuntimeType.COMPUTE + " runtime type ");
            }
            feedManager.getFeedSubscriptionManager()
                    .registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
            coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered basic feed runtime " + feedRuntime);
            }
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            try {
                if (resumeOldState) {
                    resumeOldState = false;
                }
                currentBuffer = buffer;
                coreOperatorNodePushable.nextFrame(buffer);
                currentBuffer = null;
            } catch (HyracksDataException e) {
                if (policyEnforcer.getFeedPolicyAccessor().continueOnApplicationFailure()) {
                    boolean isExceptionHarmful = e.getCause() instanceof TreeIndexException && !resumeOldState;
                    if (isExceptionHarmful) {
                        // TODO: log the tuple
                        FeedRuntimeState runtimeState = new FeedRuntimeState(buffer, writer, e);
                        // sfeedRuntime.setRuntimeState(runtimeState);
                    } else {
                        // ignore the frame (exception is expected)
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Ignoring exception " + e);
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Feed policy does not require feed to survive soft failure");
                    }
                    throw e;
                }
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
                    //feedRuntime.setRuntimeState(runtimeState);
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        // LOGGER.warning("Saved feed compute runtime for revivals" + feedRuntime.getFeedRuntimeId());
                    }
                } else {
                    //feedManager.getFeedConnectionManager().deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        //     LOGGER.warning("No state to save, de-registered feed runtime " + feedRuntime.getFeedRuntimeId());
                    }
                }
            }
            coreOperatorNodePushable.fail();
        }

        @Override
        public void close() throws HyracksDataException {
            coreOperatorNodePushable.close();
            switch (feedRuntime.getFeedRuntimeType()) {
                case STORE:
                case COMMIT:
                case COLLECT:
                    feedManager.getFeedConnectionManager().deRegisterFeedRuntime(
                            ((BasicFeedRuntime) feedRuntime).getFeedRuntimeId());
                    break;

            }
        }

    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

}
