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
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferProcessRateTimerTask;
import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask;
import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback.FrameEvent;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public abstract class MonitoredBuffer extends MessageReceiver<DataBucket> {

    protected static final long MONITOR_FREQUENCY = 2000; // 2 seconds
    protected static final long PROCESSING_RATE_MEASURE_FREQUENCY = 10000; // 10 seconds

    protected static final int PROCESS_RATE_REFRESH = 2; // refresh processing rate every 10th frame

    protected final FeedConnectionId connectionId;
    protected final FeedRuntimeId runtimeId;
    protected final FrameTupleAccessor inflowFta;
    protected final FrameTupleAccessor outflowFta;
    protected final FeedRuntimeInputHandler inputHandler;
    protected final IFrameEventCallback callback;
    protected final Timer timer;
    private final int frameSize;
    private final RecordDescriptor recordDesc;
    private final IExceptionHandler exceptionHandler;
    protected final FeedPolicyAccessor policyAccessor;
    protected int nPartitions;

    private IFrameWriter frameWriter;
    protected IFeedMetricCollector metricCollector;
    protected boolean processingRateEnabled = false;
    protected boolean trackDataMovementRate = false;
    protected int inflowReportSenderId = -1;
    protected int outflowReportSenderId = -1;
    protected TimerTask monitorTask;
    protected TimerTask processingRateTask;
    protected MonitoredBufferStorageTimerTask storageTimeTrackingRateTask;
    protected StorageFrameHandler storageFromeHandler;

    protected int processingRate = -1;
    protected int frameCount = 0;
    private long avgDelayPersistence = 0;
    private boolean active;
    private Map<Integer, Long> tupleTimeStats;

    public static MonitoredBuffer getMonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        switch (runtimeId.getFeedRuntimeType()) {
            case COMPUTE:
                return new ComputeSideMonitoredBuffer(inputHandler, frameWriter, fta, frameSize, recordDesc,
                        metricCollector, connectionId, runtimeId, exceptionHandler, callback, nPartitions,
                        policyAccessor);
            case STORE:
                return new StorageSideMonitoredBuffer(inputHandler, frameWriter, fta, frameSize, recordDesc,
                        metricCollector, connectionId, runtimeId, exceptionHandler, callback, nPartitions,
                        policyAccessor);

            default:
                return new BasicMonitoredBuffer(inputHandler, frameWriter, fta, frameSize, recordDesc, metricCollector,
                        connectionId, runtimeId, exceptionHandler, callback, nPartitions, policyAccessor);
        }
    }

    protected MonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter, FrameTupleAccessor fta,
            int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        this.connectionId = connectionId;
        this.frameWriter = frameWriter;
        this.inflowFta = new FrameTupleAccessor(frameSize, recordDesc);
        this.outflowFta = new FrameTupleAccessor(frameSize, recordDesc);
        this.runtimeId = runtimeId;
        this.metricCollector = metricCollector;
        this.exceptionHandler = exceptionHandler;
        this.callback = callback;
        this.inputHandler = inputHandler;
        this.timer = new Timer();
        this.frameSize = frameSize;
        this.recordDesc = recordDesc;
        this.policyAccessor = policyAccessor;
        this.nPartitions = nPartitions;
        this.active = true;
        initializeMonitoring();
    }

    protected abstract void initializeMonitoring();

    protected abstract void deinitializeMonitoring();

    protected abstract void postMessage(DataBucket message);

    protected abstract void preProcessFrame(ByteBuffer frame);

    protected abstract void postProcessFrame(long startTime, ByteBuffer frame);

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        postMessage(message);
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

    public FeedRuntimeInputHandler getInputHandler() {
        return inputHandler;
    }

    public synchronized void close(boolean processPending, boolean disableMonitoring) {
        super.close(processPending);
        if (disableMonitoring) {
            deinitializeMonitoring();
        }
        active = false;
    }

    @Override
    public synchronized void processMessage(DataBucket message) throws Exception {
        if (!active) {
            message.doneReading();
            return;
        }
        switch (message.getContentType()) {
            case DATA:
                boolean finishedProcessing = false;
                ByteBuffer frame = message.getContent();
                if (inputHandler.isThrottlingEnabled()) {
                    inflowFta.reset(frame);
                    int pRate = getProcessingRate();
                    int inflowRate = getInflowRate();
                    double retainFraction = (pRate * 1.0 / inflowRate);
                    if(LOGGER.isLoggable(Level.INFO)){
                        LOGGER.info("Throttling at fraction " + retainFraction);
                    }
                    frame = throttleFrame(inflowFta, retainFraction);
                }
                outflowFta.reset(frame);
                long startTime = 0;
                while (!finishedProcessing) {
                    try {
                        inflowFta.reset(frame);
                        startTime = System.currentTimeMillis();
                        preProcessFrame(frame);
                        frameWriter.nextFrame(frame);
                        postProcessFrame(startTime, frame);
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

    private ByteBuffer throttleFrame(FrameTupleAccessor fta, double retainFraction) {
        int desiredTuples = (int) (fta.getTupleCount() * retainFraction);
        return FeedFrameUtil.getSampledFrame(fta, desiredTuples, frameSize);
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

    public void reset() {
        active = true;
        if (trackDataMovementRate || processingRateEnabled) {
            metricCollector.resetReportSender(inflowReportSenderId);
            metricCollector.resetReportSender(outflowReportSenderId);
        }
    }

    public int getProcessingRate() {
        return processingRate;
    }

    public Map<Integer, Long> getTupleTimeStats() {
        return tupleTimeStats;
    }

    public long getAvgDelayRecordPersistence() {
        return avgDelayPersistence;
    }

    public MonitoredBufferStorageTimerTask getStorageTimeTrackingRateTask() {
        return storageTimeTrackingRateTask;
    }

}