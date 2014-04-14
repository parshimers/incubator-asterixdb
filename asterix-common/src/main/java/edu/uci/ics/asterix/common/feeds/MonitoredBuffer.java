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

import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class MonitoredBuffer extends MessageReceiver<DataBucket> {

    private static final long MONITOR_FREQUENCY = 2000; // 2 seconds
    
    private final IFrameWriter frameWriter;
    private final FrameTupleAccessor fta;
    private final IFeedMetricCollector metricCollector;
    private final int inflowReportSenderId;
    private final int outflowReportSenderId;
    private final FeedPolicyAccessor fpa;
    private final IExceptionHandler exceptionHandler;
    private final IFrameEventCallback callback;
    private Timer monitorTask;
    

    public MonitoredBuffer(IFrameWriter frameWriter, FrameTupleAccessor fta, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition,
            IExceptionHandler exceptionHandler, FeedPolicyAccessor fpa, IFrameEventCallback callback) {
        this.frameWriter = frameWriter;
        this.fta = fta;
        this.fpa = fpa;
        this.metricCollector = metricCollector;
        this.inflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeType, partition,
                ValueType.INFLOW_RATE, MetricType.RATE);
        this.outflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeType, partition,
                ValueType.OUTFLOW_RATE, MetricType.RATE);
        this.exceptionHandler = exceptionHandler;
        this.callback = callback;
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new MonitorTimerTask(this), 0, MONITOR_FREQUENCY);
    }

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        fta.reset(message.getBuffer());
        int nTuples = fta.getTupleCount();
        metricCollector.sendReport(inflowReportSenderId, nTuples);
    }

    @Override
    public void processMessage(DataBucket message) throws Exception {
        boolean finishedProcessing = false;
        ByteBuffer frame = message.getBuffer();
        while (!finishedProcessing) {
            try {
                switch (message.getContentType()) {
                    case DATA:
                        frameWriter.nextFrame(frame);
                        break;
                    case EOD:
                        callback.frameEvent(IFrameEventCallback.FrameEvent.FINISHED_PROCESSING);
                }
                finishedProcessing = true;
            } catch (Exception e) {
                if (fpa.continueOnSoftFailure()) {
                    frame = exceptionHandler.handleException(e, frame);
                } else {
                    throw e;
                }
            }
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
}