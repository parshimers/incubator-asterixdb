package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

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
        if (ackingEnabled || timeTrackingEnabled) {
            storageFromeHandler = new StorageFrameHandler();
            this.storageTimeTrackingRateTask = new MonitoredBufferTimerTasks.MonitoredBufferStorageTimerTask(this,
                    inputHandler.getFeedManager(), connectionId, runtimeId.getPartition(), policyAccessor,
                    storageFromeHandler);
            this.timer.scheduleAtFixedRate(storageTimeTrackingRateTask, 0, STORAGE_TIME_TRACKING_FREQUENCY);
        }
    }

    @Override
    protected boolean monitorProcessingRate() {
        return false;
    }

    protected boolean logInflowOutflowRate() {
        return true;
    }

    @Override
    public IFramePreprocessor getFramePreProcessor() {
        return new IFramePreprocessor() {

            @Override
            public void preProcess(ByteBuffer frame) {
                try {
                    if (ackingEnabled || timeTrackingEnabled) {
                        storageFromeHandler.updateTrackingInformation(frame, inflowFta);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

    @Override
    protected IFramePostProcessor getFramePostProcessor() {
        return null;
    }

    public boolean isAckingEnabled() {
        return ackingEnabled;
    }

    public boolean isTimeTrackingEnabled() {
        return timeTrackingEnabled;
    }

    @Override
    protected boolean monitorInputQueueLength() {
        return true;
    }

}
