package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class StorageSideMonitoredBuffer extends MonitoredBuffer {

    private static final long STORAGE_TIME_TRACKING_FREQUENCY = 5000; // 10 seconds

    private boolean ackingEnabled;
    private final boolean timeTrackingEnabled;

    public StorageSideMonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(inputHandler, frameWriter, fta, frameSize, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
        timeTrackingEnabled = policyAccessor.isTimeTrackingEnabled();
        ackingEnabled = policyAccessor.atleastOnceSemantics();
    }

    @Override
    protected void initializeMonitoring() {
        if (ackingEnabled || timeTrackingEnabled) {
            storageFromeHandler = new StorageFrameHandler();
            this.storageTimeTrackingRateTask = new MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask(this,
                    inputHandler.getFeedManager(), connectionId, runtimeId.getPartition(), policyAccessor,
                    storageFromeHandler);
            this.timer.scheduleAtFixedRate(storageTimeTrackingRateTask, 0, STORAGE_TIME_TRACKING_FREQUENCY);
        }
    }

    @Override
    protected void postMessage(DataBucket message) {

    }

    @Override
    protected void deinitializeMonitoring() {
        if (monitorTask != null) {
            monitorTask.cancel();
        }

        if (storageTimeTrackingRateTask != null) {
            storageTimeTrackingRateTask.cancel();
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Disabled monitoring for " + this.runtimeId);
        }

    }

    @Override
    protected void preProcessFrame(ByteBuffer frame) {
        try {
            if (ackingEnabled || timeTrackingEnabled) {
                storageFromeHandler.updateTrackingInformation(frame, inflowFta);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void postProcessFrame(long startTime, ByteBuffer frame) {
        // TODO Auto-generated method stub

    }

    public boolean isAckingEnabled() {
        return ackingEnabled;
    }

    public boolean isTimeTrackingEnabled() {
        return timeTrackingEnabled;
    }

}
