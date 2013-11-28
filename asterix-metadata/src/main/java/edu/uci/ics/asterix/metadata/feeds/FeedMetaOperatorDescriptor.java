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
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FeedFrameCollector;
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

        /** The (singleton) instance of IFeedManager **/
        private IFeedManager feedManager;

        /** The frame reader instance that reads the input frames from the distribute feed writer **/
        private FeedFrameCollector frameReader;

        /** true indicates that the runtime can be subscribed for data by other runtime instances. **/
        private final boolean enableSubscriptionMode;

        private final IHyracksTaskContext ctx;

        public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
                int partition, int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
                FeedPolicy feedPolicy, FeedRuntimeType runtimeType, boolean enableSubscriptionMode)
                throws HyracksDataException {
            this.ctx = ctx;
            this.coreOperatorNodePushable = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                    .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
            this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicy.getProperties());
            this.partition = partition;
            this.runtimeType = runtimeType;
            this.feedConnectionId = feedConnectionId;
            this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
            this.feedManager = ((IAsterixAppRuntimeContext) (IAsterixAppRuntimeContext) ctx.getJobletContext()
                    .getApplicationContext().getApplicationObject()).getFeedManager();
            this.enableSubscriptionMode = enableSubscriptionMode;

        }

        @Override
        public void open() throws HyracksDataException {
            FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, feedConnectionId, partition);
            try {
                feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
                IFeedFrameWriter mWriter = new FeedFrameWriter(ctx, writer, this, feedConnectionId, policyEnforcer,
                        nodeId, runtimeType, partition, recordDesc, feedManager);
                if (feedRuntime == null) {
                    switch (runtimeType) {
                        case COMPUTE:
                            if (enableSubscriptionMode) {
                                registerSubscribableRuntime(mWriter);
                            } else {
                                registerBasicFeedRuntime(mWriter);
                            }
                            break;
                        case COMMIT:
                        case STORE:
                            registerBasicFeedRuntime(mWriter);
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
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to initialize feed operator " + feedRuntime + " [" + partition + "]");
                }
                throw new HyracksDataException(e);
            }
        }

        private void retrieveSubscribableFeedRuntime(IFeedRuntime feedRuntime, IFeedFrameWriter frameWriter) {
            DistributeFeedFrameWriter dWriter = (DistributeFeedFrameWriter) feedRuntime.getFeedFrameWriter();
            dWriter.setWriter(writer);
            Map<IFeedFrameWriter, FeedFrameCollector> registeredReaders = dWriter.getRegisteredReaders();
            for (Entry<IFeedFrameWriter, FeedFrameCollector> entry : registeredReaders.entrySet()) {
                if (entry.getValue().equals(frameReader)) {
                    frameReader.setFrameWriter(frameWriter);
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

        private void registerSubscribableRuntime(IFeedFrameWriter feedFrameWriter) throws Exception {
            IFeedFrameWriter mWriter = new DistributeFeedFrameWriter(feedConnectionId.getFeedId(), writer, runtimeType,
                    recordDesc);
            frameReader = ((DistributeFeedFrameWriter) mWriter).subscribeFeed(feedFrameWriter);
            FeedSubscribableRuntimeId sid = new FeedSubscribableRuntimeId(feedConnectionId.getFeedId(), runtimeType,
                    partition);
            feedRuntime = new SubscribableRuntime(sid, (DistributeFeedFrameWriter) mWriter, runtimeType);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Set up distrbute feed frame writer for subscribable runtime id " + sid + " of type ["
                        + FeedRuntimeType.COMPUTE + "]");
            }
            feedManager.getFeedSubscriptionManager()
                    .registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
            coreOperatorNodePushable.setOutputFrameWriter(0, mWriter, recordDesc);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered subscribable feed runtime " + feedRuntime);
            }
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
                currentBuffer = buffer;
                coreOperatorNodePushable.nextFrame(buffer);
                currentBuffer = null;
            } catch (HyracksDataException e) {
                int checksum = computeChecksum(buffer.array(), buffer.limit());
                if (policyEnforcer.getFeedPolicyAccessor().continueOnApplicationFailure()) {
                    boolean isExceptionHarmful = e.getCause() instanceof TreeIndexException && !resumeOldState;
                    if (isExceptionHarmful) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Ignoring exception " + e + " BY " + feedConnectionId + " BUFER CHECKSUM "
                                    + checksum);
                        }
                        // TODO: log the tuple
                        FeedRuntimeState runtimeState = new FeedRuntimeState(buffer, writer, e);
                        // sfeedRuntime.setRuntimeState(runtimeState);
                    } else {
                        // ignore the frame (exception is expected)
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Ignoring exception " + e + " BY " + feedConnectionId);
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

        private static int computeChecksum(byte[] buf, int len) {
            int crc = 0xFFFF;

            for (int pos = 0; pos < len; pos++) {
                crc ^= (int) buf[pos]; // XOR byte into least sig. byte of crc

                for (int i = 8; i != 0; i--) { // Loop over each bit
                    if ((crc & 0x0001) != 0) { // If the LSB is set
                        crc >>= 1; // Shift right and XOR 0xA001
                        crc ^= 0xA001;
                    } else
                        // Else LSB is not set
                        crc >>= 1; // Just shift right
                }
            }
            // Note, this number has low and high bytes swapped, so use it accordingly (or swap bytes)
            return crc;
        }

        @Override
        public void fail() throws HyracksDataException {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.info("Core Op:" + coreOperatorNodePushable.getDisplayName() + " fail ");
            }
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (currentBuffer != null) {
                    FeedRuntimeState runtimeState = new FeedRuntimeState(currentBuffer, writer, null);
                    //feedRuntime.setFeedRuntimeState(runtimeState);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Saved feed compute runtime for revivals" + feedRuntime.getFeedId());
                    }
                } else {
                    //feedManager.getFeedConnectionManager().deRegisterFeedRuntime(feedRuntime.getFeedRuntimeId());
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.warning("No state to save, de-registered feed runtime " + feedRuntime.getFeedId());
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
                default: // do nothing

            }
        }

    }

    public IOperatorDescriptor getCoreOperator() {
        return coreOperator;
    }

}
