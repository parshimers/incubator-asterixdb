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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FrameCollection;
import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedMemoryComponent;
import edu.uci.ics.asterix.common.feeds.IFeedMemoryManager;
import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * A wrapper around the standard frame writer provided to an operator node pushable.
 * The wrapper monitors the flow of data from this operator to a downstream operator
 * over a connector. It collects statistics if required by the feed ingestion policy
 * and reports them to the Super Feed Manager chosen for the feed. In addition any
 * congestion experienced by the operator is also reported.
 */
public class FeedFrameWriter implements IFeedOperatorOutputSideHandler {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameWriter.class.getName());

    public enum Mode {
        /**
         * **
         * Normal mode of operation for an operator when
         * frames are pushed to the downstream operator.
         */
        FORWARD,

        /**
         * Failure mode of operation for an operator when
         * input frames are not pushed to the downstream operator but
         * are buffered for future retrieval. This mode is adopted
         * during failure recovery.
         */
        STORE
    }

    /** A unique identifier for the feed connection. **/
    private final FeedConnectionId feedConnectionId;

    /** Actual frame writer provided to an operator. **/
    private IFrameWriter writer;

    /** The core operator associated with the operator **/
    private IOperatorNodePushable coreOperator;

    /** set to true if health need to be monitored **/
    private final boolean reportMetrics;

    /** Mode associated with the frame writer. Possible values: FORWARD, STORE **/
    private Mode mode;

    /** The partition associated with the operator instance using the feed frame writer **/
    private int partition;

    /** Provides access to the tuples in a frame. Used in collecting statistics **/
    private FrameTupleAccessor fta;

    private final IFeedMemoryManager memoryManager;

    private final IFeedMetricCollector metricCollector;

    private final int metricSourceId;

    /** A buffer for keeping frames that are waiting to be processed **/
    private FrameCollection frames;

    private FeedRuntimeType feedRuntimeType;

    public FeedFrameWriter(IHyracksTaskContext ctx, IFrameWriter writer, IOperatorNodePushable nodePushable,
            FeedConnectionId feedConnectionId, FeedPolicyEnforcer policyEnforcer, String nodeId,
            FeedRuntimeType feedRuntimeType, int partition, RecordDescriptor outputRecordDescriptor,
            IFeedManager feedManager) {
        this.feedConnectionId = feedConnectionId;
        this.writer = writer;
        this.mode = Mode.FORWARD;
        this.coreOperator = nodePushable;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.fta = new FrameTupleAccessor(ctx.getFrameSize(), outputRecordDescriptor);
        this.memoryManager = feedManager.getFeedMemoryManager();
        this.metricCollector = feedManager.getFeedMetricCollector();
        metricSourceId = metricCollector.createReportSender(feedConnectionId, feedRuntimeType, partition,
                ValueType.OUTFLOW_RATE, MetricType.RATE);
        this.reportMetrics = feedRuntimeType.equals(FeedRuntimeType.COLLECT)
                || feedRuntimeType.equals(FeedRuntimeType.COMPUTE) || feedRuntimeType.equals(FeedRuntimeType.STORE);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        switch (mode) {
            case FORWARD:
                try {
                    writer.nextFrame(buffer);
                    if (reportMetrics) {
                        fta.reset(buffer);
                        metricCollector.sendReport(metricSourceId, fta.getTupleCount());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case STORE:
                if (frames == null) {
                    frames = (FrameCollection) memoryManager.getMemoryComponent(IFeedMemoryComponent.Type.COLLECTION);
                    if (frames == null) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Insufficient Memory: unable to store frame (discarding) for "
                                    + coreOperator.getDisplayName());
                        }
                        break;
                    }
                }
                if (frames != null) {
                    boolean success = frames.addFrame(buffer);
                    if (!success) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Insufficient Memory: unable to store frame (discarding) ");
                        }
                    }
                }
                break;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        if (frames != null) {
            memoryManager.releaseMemoryComponent(frames);
        }
        writer.close();
    }

    public void setMode(Mode newMode) throws HyracksDataException {
        if (this.mode.equals(newMode)) {
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(this + " switching to :" + newMode + " from " + this.mode);
        }
        this.mode = newMode;
        if (mode.equals(Mode.FORWARD)) {
            processBufferedRecords();
        }
    }

    private void processBufferedRecords() throws HyracksDataException {
        if (frames != null && frames.getTotalAllocation() > 0) {
            Iterator<ByteBuffer> iterator = frames.getFrameCollectionIterator();
            int tTuples = 0;
            int nTuples = 0;

            while (iterator.hasNext()) {
                ByteBuffer bufferedFrame = iterator.next();
                fta.reset(bufferedFrame);
                writer.nextFrame(bufferedFrame);
                nTuples = fta.getTupleCount();
                tTuples += nTuples;
                metricCollector.sendReport(metricSourceId, fta.getTupleCount());
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Flushed old frame (from previous failed execution) : " + nTuples + " on behalf of "
                            + feedRuntimeType + "[" + partition + "]");
                }
            }
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Flushed total of " + tTuples + " post recovery");
            }
            frames.reset();
        }
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "FeedFrameWriter (" + feedConnectionId + " [" + partition + "]" + mode + ")" + "]";
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public FeedId getFeedId() {
        return feedConnectionId.getFeedId();
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    @Override
    public Type getType() {
        return Type.BASIC_FEED_OUTPUT_HANDLER;
    }

}
