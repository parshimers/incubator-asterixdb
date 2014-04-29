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

import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.std.connectors.PartitionDataWriter;

public class MonitoredBuffer extends MessageReceiver<DataBucket> {

    private static final long MONITOR_FREQUENCY = 2000; // 2 seconds

    private IFrameWriter frameWriter;
    private final FrameTupleAccessor fta;
    private final IFeedMetricCollector metricCollector;
    private final FeedRuntimeId runtimeId;
    private final int inflowReportSenderId;
    private final int outflowReportSenderId;
    private final IExceptionHandler exceptionHandler;
    private final TimerTask monitorTask;
    private final FeedRuntimeInputHandler inputHandler;
    private final IFrameEventCallback callback;
    private final Timer timer;

    public MonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter, FrameTupleAccessor fta,
            IFeedMetricCollector metricCollector, FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            IExceptionHandler exceptionHandler, IFrameEventCallback callback) {
        this.frameWriter = frameWriter;
        this.fta = fta;
        this.runtimeId = runtimeId;
        this.metricCollector = metricCollector;
        this.inflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId, ValueType.INFLOW_RATE,
                MetricType.RATE);
        this.outflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                ValueType.OUTFLOW_RATE, MetricType.RATE);
        this.exceptionHandler = exceptionHandler;
        this.monitorTask = new MonitoredBufferTimerTask(this, callback);
        this.callback = callback;
        this.inputHandler = inputHandler;
        this.timer = new Timer();
        this.timer.scheduleAtFixedRate(monitorTask, 0, MONITOR_FREQUENCY);
    }

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        fta.reset(message.getBuffer());
        metricCollector.sendReport(inflowReportSenderId, fta.getTupleCount());
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

    @Override
    public void processMessage(DataBucket message) throws Exception {
        switch (message.getContentType()) {
            case DATA:
                boolean finishedProcessing = false;
                ByteBuffer frame = message.getBuffer();
                while (!finishedProcessing) {
                    try {
                        frameWriter.nextFrame(frame);
                        System.out.println("Writing " + this.runtimeId + " USING " + frameWriter);
                        finishedProcessing = true;
                        metricCollector.sendReport(outflowReportSenderId, fta.getTupleCount());
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
}