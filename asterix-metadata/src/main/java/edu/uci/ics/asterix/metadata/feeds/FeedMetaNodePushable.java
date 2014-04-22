package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedFrameCollector;
import edu.uci.ics.asterix.common.feeds.FeedOperatorInputSideHandler;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.SubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
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

    private FeedOperatorInputSideHandler inputSideHandler;

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
        this.feedConnectionId = feedConnectionId;
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
        FeedRuntimeId runtimeId = new FeedRuntimeId(feedConnectionId, runtimeType, operandId, partition);
        try {
            feedRuntime = feedManager.getFeedConnectionManager().getFeedRuntime(runtimeId);
            IFeedOperatorOutputSideHandler mWriter = new FeedFrameWriter(ctx, writer, this, runtimeId, policyEnforcer,
                    nodeId, recordDesc, feedManager);
            fta = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
            this.inputSideHandler = new FeedOperatorInputSideHandler(runtimeId, coreOperator,
                    policyEnforcer.getFeedPolicyAccessor(), inputSideBufferring, ctx.getFrameSize(), fta, recordDesc,
                    feedManager);

            if (feedRuntime == null) {
                switch (runtimeType) {
                    case COMPUTE:
                        registerSubscribableRuntime(mWriter);
                        break;
                    case STORE:
                    case COMMIT:
                        setupBasicRuntime(mWriter, true);
                        break;
                    case OTHER:
                        setupBasicRuntime(mWriter, false);
                        break;
                    default:
                        throw new IllegalStateException("Invalid wrapping of " + runtimeType + " by meta feed operator");
                }
            } else {
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
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Retreived state from the zombie instance from previous execution for "
                            + runtimeType + " node.");
                }
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

    private void setupBasicRuntime(IFeedOperatorOutputSideHandler mWriter, boolean register) throws Exception {
        coreOperator.setOutputFrameWriter(0, mWriter, recordDesc);
        if (register) {
            feedRuntime = new BasicFeedRuntime(feedConnectionId, partition, mWriter, runtimeType, operandId);
            feedManager.getFeedConnectionManager().registerFeedRuntime((BasicFeedRuntime) feedRuntime);
        }
    }

    private void registerSubscribableRuntime(IFeedOperatorOutputSideHandler feedFrameWriter) throws Exception {
        FeedSubscribableRuntimeId sid = new FeedSubscribableRuntimeId(feedConnectionId.getFeedId(), runtimeType,
                partition);
        DistributeFeedFrameWriter distributeWriter = new DistributeFeedFrameWriter(feedConnectionId.getFeedId(),
                writer, runtimeType, partition, new FrameTupleAccessor(ctx.getFrameSize(), recordDesc), feedManager,
                ctx.getFrameSize());
        outputSideFrameCollector = distributeWriter.subscribeFeed(policyEnforcer.getFeedPolicyAccessor(),
                feedFrameWriter, feedConnectionId);
        feedRuntime = new SubscribableRuntime(sid, distributeWriter, runtimeType, recordDesc);
        feedManager.getFeedSubscriptionManager().registerFeedSubscribableRuntime((ISubscribableRuntime) feedRuntime);
        coreOperator.setOutputFrameWriter(0, distributeWriter, recordDesc);
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

}