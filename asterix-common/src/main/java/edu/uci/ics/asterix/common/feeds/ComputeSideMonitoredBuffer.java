package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class ComputeSideMonitoredBuffer extends MonitoredBuffer {

    public ComputeSideMonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(inputHandler, frameWriter, fta, frameSize, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
    }

    @Override
    protected void initializeMonitoring() {
        processingRateEnabled = true;
        trackDataMovementRate = true;
        if (trackDataMovementRate) {
            this.monitorTask = new MonitoredBufferTimerTasks.MonitoredBufferDataFlowRateMeasureTimerTask(this, callback);
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
    protected void postMessage(DataBucket message) {
        if (trackDataMovementRate
                && !(inputHandler.getMode().equals(Mode.PROCESS_BACKLOG) || inputHandler.getMode().equals(
                        Mode.PROCESS_SPILL))) {
            inflowFta.reset(message.getContent());
            metricCollector.sendReport(inflowReportSenderId, inflowFta.getTupleCount());
        }

    }

    @Override
    protected void deinitializeMonitoring() {
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

    @Override
    protected void preProcessFrame(ByteBuffer frame) {

    }

    @Override
    protected void postProcessFrame(long startTime, ByteBuffer frame) {
        if (processingRateEnabled) {
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

        if (trackDataMovementRate) {
            metricCollector.sendReport(outflowReportSenderId, outflowFta.getTupleCount());
        }

    }

}
