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

import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferProcessRateTimerTask;
import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class MonitoredBuffer extends MessageReceiver<DataBucket> {

    private static final long MONITOR_FREQUENCY = 2000; // 2 seconds
    private static final long PROCESSING_RATE_MEASURE_FREQUENCY = 10000; // 10 seconds

    private IFrameWriter frameWriter;
    private final FrameTupleAccessor inflowFta;
    private final FrameTupleAccessor outflowFta;
    private final IFeedMetricCollector metricCollector;
    private final FeedRuntimeId runtimeId;
    private final int inflowReportSenderId;
    private final int outflowReportSenderId;
    private final IExceptionHandler exceptionHandler;
    private TimerTask monitorTask;
    private TimerTask processingRateTask;
    private final FeedRuntimeInputHandler inputHandler;
    private final IFrameEventCallback callback;
    private final Timer timer;

    private boolean evaluateProcessingRate;
    private int processingRate = -1;

    public MonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter, FrameTupleAccessor fta,
            int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions) {
        this.frameWriter = frameWriter;
        this.inflowFta = fta;
        this.outflowFta = new FrameTupleAccessor(frameSize, recordDesc);
        this.runtimeId = runtimeId;
        this.metricCollector = metricCollector;
        this.inflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId, ValueType.INFLOW_RATE,
                MetricType.RATE);
        this.outflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                ValueType.OUTFLOW_RATE, MetricType.RATE);
        this.exceptionHandler = exceptionHandler;

        this.callback = callback;
        this.inputHandler = inputHandler;
        this.timer = new Timer();
        switch (runtimeId.getFeedRuntimeType()) {
            case COMPUTE:
                this.processingRateTask = new MonitoredBufferTimerTasks.MonitoredBufferProcessRateTimerTask(this,
                        inputHandler.getFeedManager(), connectionId, nPartitions);
                this.timer.scheduleAtFixedRate(processingRateTask, 0, PROCESSING_RATE_MEASURE_FREQUENCY);
                this.monitorTask = new MonitoredBufferTimerTasks.MonitoredBufferTimerTask(this, callback);
                this.timer.scheduleAtFixedRate(monitorTask, 0, MONITOR_FREQUENCY);
                break;
            case STORE:
                this.monitorTask = new MonitoredBufferTimerTasks.MonitoredBufferTimerTask(this, callback);
                this.timer.scheduleAtFixedRate(monitorTask, 0, MONITOR_FREQUENCY);
                break;
            default:
                break;
        }
    }

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        inflowFta.reset(message.getBuffer());
        metricCollector.sendReport(inflowReportSenderId, inflowFta.getTupleCount());
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
            ((MonitoredBufferProcessRateTimerTask) processingRateTask).setNumberOfPartitions(nPartitions);
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
                        if (evaluateProcessingRate) {
                            startTime = System.currentTimeMillis();
                        }
                        frameWriter.nextFrame(frame);
                        if (evaluateProcessingRate) {
                            endTime = System.currentTimeMillis();
                            int currentRate = (int) ((double) outflowFta.getTupleCount() * 1000 / (endTime - startTime));
                            if (processingRate > 0) {
                                processingRate = (processingRate + currentRate) / 2;
                            } else {
                                processingRate = currentRate;
                            }
                            evaluateProcessingRate = false;
                        }
                        finishedProcessing = true;
                        metricCollector.sendReport(outflowReportSenderId, outflowFta.getTupleCount());
                    } catch (Exception e) {
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
        metricCollector.resetReportSender(inflowReportSenderId);
        metricCollector.resetReportSender(outflowReportSenderId);
    }

    public int getProcessingRate() {
        return processingRate;
    }

    public void setProcessingRate(int processingRate) {
        this.processingRate = processingRate;
    }

    public boolean isEvaluateProcessingRate() {
        return evaluateProcessingRate;
    }

    public void setEvaluateProcessingRate(boolean evaluateProcessingRate) {
        this.evaluateProcessingRate = evaluateProcessingRate;
    }
}