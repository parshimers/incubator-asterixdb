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

import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.LogInputOutputRateTask;
import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitorInputQueueLengthTimerTask;
import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoreProcessRateTimerTask;
import edu.uci.ics.asterix.common.feeds.MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask;
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

public abstract class MonitoredBuffer extends MessageReceiver<DataBucket> {

    protected static final long LOG_INPUT_OUTPUT_RATE_FREQUENCY = 1000; // 1 second
    protected static final long INPUT_QUEUE_MEASURE_FREQUENCY = 1000; // 1 second
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
    protected boolean monitorProcessingRate = false;
    protected boolean monitorInputQueueLength = false;
    protected boolean logInflowOutflowRate = false;

    protected int inflowReportSenderId = -1;
    protected int outflowReportSenderId = -1;
    protected TimerTask monitorInputQueueLengthTask;
    protected TimerTask processingRateTask;
    protected TimerTask logInflowOutflowRateTask;
    protected MonitoredBufferStorageTimerTask storageTimeTrackingRateTask;
    protected StorageFrameHandler storageFromeHandler;

    protected int processingRate = -1;
    protected int frameCount = 0;
    private long avgDelayPersistence = 0;
    private boolean active;
    private Map<Integer, Long> tupleTimeStats;
    IFramePostProcessor postProcessor = null;
    IFramePreprocessor preProcessor = null;

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

    protected abstract boolean monitorProcessingRate();

    protected abstract boolean logInflowOutflowRate();

    protected abstract boolean monitorInputQueueLength();

    protected abstract IFramePreprocessor getFramePreProcessor();

    protected abstract IFramePostProcessor getFramePostProcessor();

    protected void initializeMonitoring() {
        monitorProcessingRate = monitorProcessingRate();
        monitorInputQueueLength = monitorInputQueueLength();
        logInflowOutflowRate = logInflowOutflowRate();

        if (monitorProcessingRate && policyAccessor.isElastic()) { // check posibilty to scale in
            this.processingRateTask = new MonitoreProcessRateTimerTask(this, inputHandler.getFeedManager(),
                    connectionId, nPartitions);
            this.timer.scheduleAtFixedRate(processingRateTask, 0, PROCESSING_RATE_MEASURE_FREQUENCY);
        }

        if (monitorInputQueueLength
                && (policyAccessor.isElastic() || policyAccessor.throttlingEnabled()
                        || policyAccessor.spillToDiskOnCongestion() || policyAccessor.discardOnCongestion())) {
            this.monitorInputQueueLengthTask = new MonitorInputQueueLengthTimerTask(this, callback);
            this.timer.scheduleAtFixedRate(monitorInputQueueLengthTask, 0, INPUT_QUEUE_MEASURE_FREQUENCY);
        }

        if (logInflowOutflowRate) {
            this.logInflowOutflowRateTask = new LogInputOutputRateTask(this);
            this.timer.scheduleAtFixedRate(logInflowOutflowRateTask, 0, LOG_INPUT_OUTPUT_RATE_FREQUENCY);
            this.inflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                    ValueType.INFLOW_RATE, MetricType.RATE);
            this.outflowReportSenderId = metricCollector.createReportSender(connectionId, runtimeId,
                    ValueType.OUTFLOW_RATE, MetricType.RATE);
        }
    }

    protected void deinitializeMonitoring() {
        if (monitorInputQueueLengthTask != null) {
            monitorInputQueueLengthTask.cancel();
        }
        if (processingRateTask != null) {
            processingRateTask.cancel();
        }

        if (logInflowOutflowRate) {
            metricCollector.removeReportSender(inflowReportSenderId);
            metricCollector.removeReportSender(outflowReportSenderId);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Disabled monitoring for " + this.runtimeId);
        }
    }

    protected void postProcessFrame(long startTime, ByteBuffer frame) throws Exception {
        if (monitorProcessingRate) {
            frameCount++;
            if (frameCount % PROCESS_RATE_REFRESH == 0) {
                long endTime = System.currentTimeMillis();
                processingRate = (int) ((double) outflowFta.getTupleCount() * 1000 / (endTime - startTime));
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Processing Rate :" + processingRate + " tuples/sec");
                }
                frameCount = 0;
            }
        }

        if (logInflowOutflowRate) {
            metricCollector.sendReport(outflowReportSenderId, outflowFta.getTupleCount());
        }

        postProcessFrame(frame);

    }

    protected void preProcessFrame(ByteBuffer frame) throws Exception {
        if (postProcessor == null) {
            preProcessor = getFramePreProcessor();
        }
        if (preProcessor != null) {
            preProcessor.preProcess(frame);
        }
    }

    protected void postProcessFrame(ByteBuffer frame) throws Exception {
        if (postProcessor == null) {
            postProcessor = getFramePostProcessor();
        }
        if (postProcessor != null) {
            outflowFta.reset(frame);
            postProcessor.postProcessFrame(frame, outflowFta);
        }
    }

    @Override
    public void sendMessage(DataBucket message) {
        inbox.add(message);
        if (logInflowOutflowRate
                && !(inputHandler.getMode().equals(Mode.PROCESS_BACKLOG) || inputHandler.getMode().equals(
                        Mode.PROCESS_SPILL))) {
            inflowFta.reset(message.getContent());
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
            int currentPartitions = ((MonitoreProcessRateTimerTask) processingRateTask).getNumberOfPartitions();
            if (currentPartitions != nPartitions) {
                ((MonitoreProcessRateTimerTask) processingRateTask).setNumberOfPartitions(nPartitions);
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
                ByteBuffer frameReceived = message.getContent();
                ByteBuffer frameToProcess = null;
                if (inputHandler.isThrottlingEnabled()) {
                    inflowFta.reset(frameReceived);
                    int pRate = getProcessingRate();
                    int inflowRate = getInflowRate();
                    if (inflowRate > pRate) {
                        double retainFraction = (pRate * 0.8 / inflowRate);
                        frameToProcess = throttleFrame(inflowFta, retainFraction);
                        inflowFta.reset(frameToProcess);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Throttling at fraction " + retainFraction + " no of tuples remaining "
                                    + inflowFta.getTupleCount());

                        }
                    } else {
                        frameToProcess = frameReceived;
                    }
                } else {
                    frameToProcess = frameReceived;
                }
                outflowFta.reset(frameToProcess);
                long startTime = 0;
                while (!finishedProcessing) {
                    try {
                        inflowFta.reset(frameToProcess);
                        startTime = System.currentTimeMillis();
                        preProcessFrame(frameToProcess);
                        frameWriter.nextFrame(frameToProcess);
                        postProcessFrame(startTime, frameToProcess);
                        finishedProcessing = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        frameToProcess = exceptionHandler.handleException(e, frameToProcess);
                        finishedProcessing = true;
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
        int startTupleIndex = fta.getTupleCount() - desiredTuples;
        //return FeedFrameUtil.getSampledFrame(fta, desiredTuples, frameSize);
        return FeedFrameUtil.getSlicedFrame(frameSize, startTupleIndex, fta);

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
        if (logInflowOutflowRate) {
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