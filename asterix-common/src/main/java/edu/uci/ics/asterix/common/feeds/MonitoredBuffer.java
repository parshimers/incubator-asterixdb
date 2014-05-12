/*
 * Copyright 2009-2014 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferProcessRateTimerTask;
import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class MonitoredBuffer extends MessageReceiver<DataBucket> {

    private static final long MONITOR_FREQUENCY = 2000; // 2 seconds
    private static final long PROCESSING_RATE_MEASURE_FREQUENCY = 10000; // 10 seconds
    private static final int PROCESS_RATE_REFRESH = 10; // refresh processing rate every 10th frame

    private IFrameWriter frameWriter;
    private final FrameTupleAccessor inflowFta;
    private final FrameTupleAccessor outflowFta;
    private IFeedMetricCollector metricCollector;
    private final FeedRuntimeId runtimeId;
    private boolean processingRateEnabled = false;
    private boolean trackDataMovementRate = false;
    private int inflowReportSenderId = -1;
    private int outflowReportSenderId = -1;
    private final IExceptionHandler exceptionHandler;
    private TimerTask monitorTask;
    private TimerTask processingRateTask;
    private final FeedRuntimeInputHandler inputHandler;
    private final IFrameEventCallback callback;
    private final Timer timer;
    private int processingRate = -1;
    private int frameCount = 0;

    public MonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter, FrameTupleAccessor fta,
            int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions) {
        this.frameWriter = frameWriter;
        this.inflowFta = fta;
        this.outflowFta = new FrameTupleAccessor(frameSize, recordDesc);
        this.runtimeId = runtimeId;
        this.metricCollector = metricCollector;
        this.exceptionHandler = exceptionHandler;
        this.callback = callback;
        this.inputHandler = inputHandler;
        this.timer = new Timer();
        switch (runtimeId.getFeedRuntimeType()) {
            case COMPUTE:
                processingRateEnabled = true;
                trackDataMovementRate = true;
                break;
            default:
                break;
        }
        if (trackDataMovementRate) {
            this.monitorTask = new MonitoredBufferTimerTasks.MonitoredBufferTimerTask(this, callback);
            this.timer.scheduleAtFixedRate(monitorTask, 0, MONITOR_FREQUENCY);
        }
        if (processingRateEnabled) {
            this.processingRateTask = new MonitoredBufferTimerTasks.MonitoredBufferProcessRateTimerTask(this,
                    inputHandler.getFeedManager(), connectionId, nPartitions);
            this.timer.scheduleAtFixedRate(processingRateTask, 0, PROCESSING_RATE_MEASURE_FREQUENCY);
        }
        if (trackDataMovementRate || processingRateEnabled) {
            this.inflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                    ValueType.INFLOW_RATE, MetricType.RATE);
            this.outflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                    ValueType.OUTFLOW_RATE, MetricType.RATE);
        }

    }

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        if (trackDataMovementRate
                && !(inputHandler.getMode().equals(Mode.PROCESS_BACKLOG) || inputHandler.getMode().equals(
                        Mode.PROCESS_SPILL))) {
            inflowFta.reset(message.getBuffer());
            metricCollector.sendReport(inflowReportSenderId, inflowFta.getTupleCount());
        }
    }

    /** return rate in terms of tuples/sec **/
    public int getInflowRate() {
        return metricCollector.getMetric(inflowReportSenderId);
    }

    /** return rate in terms of tuples/sec **/
    public int getOutflowRate() {
        return metricCollector.getMetric(outflowReportSenderId);
    }

    /** return the number of pending frames from the input queue **/
    public int getWorkSize() {
        return inbox.size();
    }

    /** reset the number of partitions (cardinality) for the runtime **/
    public void setNumberOfPartitions(int nPartitions) {
        if (processingRateTask != null) {
            int currentPartitions = ((MonitoredBufferProcessRateTimerTask) processingRateTask).getNumberOfPartitions();
            if (currentPartitions != nPartitions) {
                ((MonitoredBufferProcessRateTimerTask) processingRateTask).setNumberOfPartitions(nPartitions);
            }
        }
    }

    public void close(boolean processPending, boolean disableMonitoring) {
        super.close(processPending);
        if (disableMonitoring) {
            if (monitorTask != null) {
                monitorTask.cancel();
            }
            if (processingRateTask != null) {
                processingRateTask.cancel();
            }

            if (trackDataMovementRate || processingRateEnabled) {
                metricCollector.removeReportSender(inflowReportSenderId);
                metricCollector.removeReportSender(outflowReportSenderId);
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Disabled monitoring for " + this.runtimeId);
            }
        }
    }

    @Override
    public void processMessage(DataBucket message) throws Exception {
        switch (message.getContentType()) {
            case DATA:
                boolean finishedProcessing = false;
                ByteBuffer frame = message.getBuffer();
                outflowFta.reset(frame);
                long startTime = 0;
                long endTime = 0;
                while (!finishedProcessing) {
                    try {
                        inflowFta.reset(frame);
                        startTime = System.currentTimeMillis();
                        if (runtimeId.getFeedRuntimeType().equals(FeedRuntimeType.STORE)) {
                            updateStorageTimestamp(frame);
                        }
                        frameWriter.nextFrame(frame);
                        if (processingRateEnabled) {
                            frameCount++;
                            if (frameCount % PROCESS_RATE_REFRESH == 0) {
                                endTime = System.currentTimeMillis();
                                processingRate = (int) ((double) outflowFta.getTupleCount() * 1000 / (endTime - startTime));
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Processing Rate :" + processingRate + " tuples/sec");
                                }
                                frameCount = 0;
                            }
                        }
                        if (trackDataMovementRate) {
                            metricCollector.sendReport(outflowReportSenderId, outflowFta.getTupleCount());
                        }
                        finishedProcessing = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        frame = exceptionHandler.handleException(e, frame);
                    }
                }
                message.doneReading();
                break;
            case EOD:
                message.doneReading();
                timer.cancel();
                callback.frameEvent(FrameEvent.FINISHED_PROCESSING);
                break;
            case EOSD:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Done processing spillage");
                }
                message.doneReading();
                callback.frameEvent(FrameEvent.FINISHED_PROCESSING_SPILLAGE);
                break;

        }
    }

    private void updateStorageTimestamp(ByteBuffer frame) throws IOException {
        int nTuples = inflowFta.getTupleCount();
        for (int i = 0; i < nTuples; i++) {
            int recordStart = inflowFta.getTupleStartOffset(i) + inflowFta.getFieldSlotsLength();
            int openPartOffsetOrig = frame.getInt(recordStart + 6);
            int storageValueOffset = openPartOffsetOrig + 4 + 8 * 2 + 19 + 9 + 19 + 1;
            frame.putLong(recordStart + storageValueOffset, System.currentTimeMillis());
        }
    }

    /*
    private void writeTimestampedFrame(ByteBuffer frame) throws IOException {
        appender.reset(timestamped, false);
        int nTuples = inflowFta.getTupleCount();
        boolean s = false;
        for (int i = 0; i < nTuples; i++) {
            tb.reset();
            int startOffset = inflowFta.getTupleStartOffset(i) + inflowFta.getFieldSlotsLength();
            int endOffset = inflowFta.getTupleEndOffset(i);
            createModifiedRecord(frame, startOffset, endOffset, tb);
            addTupleToFrame(timestamped);
        }
    }*/

    private static void createModifiedRecord(ByteBuffer b, int tupleStart, int tupleEnd, ArrayTupleBuilder tb)
            throws IOException {
        int rIndex = 0;

        int nTupleFields = 2;
        int addHashCode = 4;
        int addOffset = 4;
        int addNameLength = "storage-timestamp".length();
        int addValueLength = 8;
        int addNamePrefix = 2;
        int addValuePrefix = 1;
        int deltaLength = addHashCode + addOffset + addNameLength + addValueLength + addNamePrefix + addValuePrefix;

        int recordStart = tupleStart + 8;
        int origRecordLength = (tupleEnd - recordStart + 1);
        ByteBuffer tupleBuffer = ByteBuffer.allocate(origRecordLength + 8 + deltaLength);
        tupleBuffer.putInt(b.getInt(tupleStart) + deltaLength); // tuple field end offset 1
        tupleBuffer.putInt(b.getInt(tupleStart + 4) + deltaLength); // tuple field end offset 2
        tupleBuffer.put(b.get(recordStart)); // record header
        tupleBuffer.putInt(b.getInt(recordStart + 1) + deltaLength); // length of record

        int openPartOffsetOrig = b.getInt(recordStart + 6);
        int identicalStartPos = nTupleFields * 4 + 1 + 4;
        int indenticalEndPos = recordStart + openPartOffsetOrig - 1;
        int nSame = indenticalEndPos - identicalStartPos + 1;
        tupleBuffer.put(b.array(), identicalStartPos, nSame); // write uptil closed part

        rIndex = openPartOffsetOrig;
        tupleBuffer.putInt(2); // number of open fields incremented by 1
        rIndex += 4;

        int lenComputeName = (int) b.get(recordStart + openPartOffsetOrig + 13);

        tupleBuffer.put(new byte[] { 39, -39, -80, 68 });// hashCode of the 1st field (storage timestamp)
        int secondFieldOffset = tupleBuffer.position() + 4 + 8 + 2 + lenComputeName + 9 - recordStart;
        tupleBuffer.putInt(secondFieldOffset); // offset of second field

        tupleBuffer.putInt(b.getInt(recordStart + openPartOffsetOrig + 4)); // hash code for 1st field
        tupleBuffer.putInt(b.getInt(recordStart + openPartOffsetOrig + 8)); // offset for 1st field 
        rIndex += 8;

        tupleBuffer.put(b.array(), recordStart + rIndex, lenComputeName + 2); // copy the string "compute-timestamp"
        rIndex += 2 + lenComputeName;

        tupleBuffer.put(b.array(), recordStart + rIndex, 1 + 8); // copy long
        rIndex += 9;

        tupleBuffer.put((byte) 0);
        tupleBuffer.put((byte) "storage-timestamp".length()); // copy the string "storage-timestamp"
        tupleBuffer.put("storage-timestamp".getBytes());
        tupleBuffer.put((byte) 4);
        tupleBuffer.putLong(System.currentTimeMillis()); // copy long value

        int startOfRemainingContent = recordStart + rIndex;
        tupleBuffer.put(b.array(), startOfRemainingContent, tupleEnd - startOfRemainingContent + 1);
        //   tupleBuffer.put

        tupleBuffer.flip();

        for (int i = 0; i < tupleBuffer.array().length; i++) {
            System.out.print(tupleBuffer.array()[i] + ",");
        }
        tb.getDataOutput().write(tupleBuffer.array());
    }

    public Mode getMode() {
        return inputHandler.getMode();
    }

    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    public void setFrameWriter(IFrameWriter frameWriter) {
        this.frameWriter = frameWriter;
    }

    public void resetMetrics() {
        if (trackDataMovementRate || processingRateEnabled) {
            metricCollector.resetReportSender(inflowReportSenderId);
            metricCollector.resetReportSender(outflowReportSenderId);
        }
    }

    public int getProcessingRate() {
        return processingRate;
    }

}