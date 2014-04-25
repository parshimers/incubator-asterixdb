package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeInputHandler;
import edu.uci.ics.asterix.common.feeds.FeedRuntime;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.SubscribableFeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.SubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class FeedMetaNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {

    private static final Logger LOGGER = Logger.getLogger(FeedMetaNodePushable.class.getName());

    /** Runtime node pushable corresponding to the core feed operator **/
    private AbstractUnaryInputUnaryOutputOperatorNodePushable coreOperator;

    /**
     * A policy enforcer that ensures dyanmic decisions for a feed are taken
     * in accordance with the associated ingestion policy
     **/
    private FeedPolicyEnforcer policyEnforcer;

    /**
     * The Feed Runtime instance associated with the operator. Feed Runtime
     * captures the state of the operator while the feed is active.
     */
    private FeedRuntime feedRuntime;

    /**
     * A unique identifier for the feed instance. A feed instance represents
     * the flow of data from a feed to a dataset.
     **/
    private FeedConnectionId connectionId;

    /**
     * Denotes the i'th operator instance in a setting where K operator
     * instances are scheduled to run in parallel
     **/
    private int partition;

    /** Type associated with the core feed operator **/
    private final FeedRuntimeType runtimeType;

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

    private FrameTupleAccessor fta;

    private final IHyracksTaskContext ctx;

    private final String operandId;

    private FeedRuntimeInputHandler inputSideHandler;

    public FeedMetaNodePushable(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition,
            int nPartitions, IOperatorDescriptor coreOperator, FeedConnectionId feedConnectionId,
            Map<String, String> feedPolicyProperties, FeedRuntimeType runtimeType, boolean enableSubscriptionMode,
            String operationId) throws HyracksDataException {
        this.ctx = ctx;
        this.coreOperator = (AbstractUnaryInputUnaryOutputOperatorNodePushable) ((IActivity) coreOperator)
                .createPushRuntime(ctx, recordDescProvider, partition, nPartitions);
        this.policyEnforcer = new FeedPolicyEnforcer(feedConnectionId, feedPolicyProperties);
        this.partition = partition;
        this.runtimeType = runtimeType;
        this.connectionId = feedConnectionId;
        this.nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        this.feedManager = ((IAsterixAppRuntimeContext) (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject()).getFeedManager();
        this.enableSubscriptionMode = enableSubscriptionMode;
        this.inputSideBufferring = runtimeType.equals(FeedRuntimeType.COMPUTE)
                || runtimeType.equals(FeedRuntimeType.STORE);
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedManager = runtimeCtx.getFeedManager();
        this.operandId = operationId;
    }

    @Override
    public void open() throws HyracksDataException {
        FeedRuntimeId runtimeId = getRuntimeId(runtimeType, partition, operandId);
        try {
            IFeedOperatorOutputSideHandler mWriter = new FeedFrameWriter(ctx, writer, this, connectionId, runtimeId,
                    policyEnforcer, nodeId, recordDesc, feedManager);
            feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(connectionId, runtimeId);
            if (feedRuntime == null) {
                initializeNewFeedRuntime(mWriter, runtimeId);
            } else {
                reviveOldFeedRuntime(mWriter, runtimeId);
            }

            coreOperator.open();
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Unable to initialize feed operator " + runtimeType + " [" + partition + "]");
            }
            throw new HyracksDataException(e);
        }
    }

    private void initializeNewFeedRuntime(IFeedOperatorOutputSideHandler mWriter, FeedRuntimeId runtimeId)
            throws Exception {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Runtime not found for  " + runtimeId + " connection id " + connectionId);
        }
        this.fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
        this.inputSideHandler = new FeedRuntimeInputHandler(connectionId, runtimeId, coreOperator,
                policyEnforcer.getFeedPolicyAccessor(), inputSideBufferring, ctx.getFrameSize(), fta, recordDesc,
                feedManager);

        switch (runtimeType) {
            case COMPUTE:
                setupSubscribableRuntime(inputSideHandler, mWriter, runtimeId);
                break;
            case STORE:
            case COMMIT:
                setupBasicRuntime(inputSideHandler, mWriter, true);
                break;
            case OTHER:
                setupBasicRuntime(inputSideHandler, mWriter, false);
                break;
            default:
                throw new IllegalStateException("Invalid wrapping of " + runtimeType + " by meta feed operator");
        }
    }

    private void reviveOldFeedRuntime(IFeedOperatorOutputSideHandler mWriter, FeedRuntimeId runtimeId) {
        switch (feedRuntime.getMode()) {
            case FAIL:
            case CLOSED:
                break;
            default:
                waitTillFailOrClosed();
                break;
        }
        switch (runtimeType) {
            case COMPUTE:
                retrieveSubscribableFeedRuntime(feedRuntime, mWriter);
                break;
            case OTHER:
            case COMMIT:
            case STORE:
                coreOperator.setOutputFrameWriter(0, mWriter, recordDesc);
                break;
        }
        feedRuntime.setMode(Mode.PROCESS);
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Retreived state from the zombie instance from previous execution for " + runtimeType
                    + " node.");
        }
    }

    private void waitTillFailOrClosed() {

    }

    private FeedRuntimeId getRuntimeId(FeedRuntimeType runtimeType, int partition, String operandId) {
        switch (runtimeType) {
            case COMPUTE:
                SubscribableFeedRuntimeId sid = new SubscribableFeedRuntimeId(connectionId.getFeedId(), runtimeType,
                        partition);
                return sid;
            case STORE:
            case COMMIT:
            case COMPUTE_COLLECT:
            case OTHER:
                return new FeedRuntimeId(runtimeType, partition, operandId);
            default:
                return null;
        }
    }

    private void retrieveSubscribableFeedRuntime(IFeedRuntime feedRuntime, IFeedOperatorOutputSideHandler frameWriter) {
        DistributeFeedFrameWriter dWriter = (DistributeFeedFrameWriter) feedRuntime.getFeedFrameWriter();
        dWriter.setWriter(writer);

        Map<IFrameWriter, FeedFrameCollector> registeredReaders = dWriter.getRegisteredReaders();
        for (Entry<IFrameWriter, FeedFrameCollector> entry : registeredReaders.entrySet()) {
            if (entry.getValue().equals(outputSideFrameCollector)) {
                outputSideFrameCollector.setFrameWriter(frameWriter);
                break;
            }
        }
        coreOperator.setOutputFrameWriter(0, dWriter, recordDesc);
    }

    private void setupBasicRuntime(FeedRuntimeInputHandler inputHandler, IFeedOperatorOutputSideHandler mWriter,
            boolean register) throws Exception {
        coreOperator.setOutputFrameWriter(0, mWriter, recordDesc);
        if (register) {
            FeedRuntimeId runtimeId = new FeedRuntimeId(runtimeType, partition, operandId);
            feedRuntime = new FeedRuntime(runtimeId, inputHandler, mWriter);
            feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, (FeedRuntime) feedRuntime);
        }
    }

    private void setupSubscribableRuntime(FeedRuntimeInputHandler inputHandler,
            IFeedOperatorOutputSideHandler feedFrameWriter, FeedRuntimeId runtimeId) throws Exception {
        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(connectionId.getFeedId(), writer,
                runtimeType, partition, new FrameTupleAccessor(ctx.getFrameSize(), recordDesc), feedManager,
                ctx.getFrameSize());
        coreOperator.setOutputFrameWriter(0, distributeWriter, recordDesc);

        feedRuntime = new SubscribableRuntime(connectionId.getFeedId(), runtimeId, inputHandler, distributeWriter,
                recordDesc);
        feedManager.getFeedSubscriptionManager().registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
        feedManager.getFeedConnectionManager().registerFeedRuntime(connectionId, feedRuntime);

        outputSideFrameCollector = distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(),
                feedFrameWriter, connectionId);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            inputSideHandler.nextFrame(buffer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.info("Core Op:" + coreOperator.getDisplayName() + " fail ");
        }
        coreOperator.fail();
        inputSideHandler.close();
        feedRuntime.setMode(Mode.FAIL);
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (inputSideBufferring) {
                inputSideHandler.nextFrame(null); // signal end of data
                while (!inputSideHandler.isFinished()) {
                    synchronized (coreOperator) {
                        coreOperator.wait();
                    }
                }
                coreOperator.close();
            } else {
                coreOperator.close();
            }
            inputSideHandler.setMode(Mode.CLOSED);
        } catch (Exception e) {
            e.printStackTrace();
            // ignore
        } finally {
            deregister();
            inputSideHandler.close();
        }
    }

    private void deregister() {
        if (feedRuntime != null) {
            switch (feedRuntime.getRuntimeId().getFeedRuntimeType()) {
                case STORE:
                case COMMIT:
                case COLLECT:
                    feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId,
                            ((FeedRuntime) feedRuntime).getRuntimeId());
                    break;
                case COMPUTE:
                    if (enableSubscriptionMode) {
                        SubscribableFeedRuntimeId runtimeId = (SubscribableFeedRuntimeId) feedRuntime.getRuntimeId();
                        feedManager.getFeedSubscriptionManager().deregisterFeedSubscribableRuntime(runtimeId);
                    } else {
                        feedManager.getFeedConnectionManager().deRegisterFeedRuntime(connectionId,
                                ((FeedRuntime) feedRuntime).getRuntimeId());
                    }
                    break;
            }
        }
    }

}