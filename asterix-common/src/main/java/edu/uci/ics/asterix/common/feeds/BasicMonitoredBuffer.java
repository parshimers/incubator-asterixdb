package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class BasicMonitoredBuffer extends MonitoredBuffer {

    public BasicMonitoredBuffer(FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter, FrameTupleAccessor fta,
            int frameSize, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(inputHandler, frameWriter, fta, frameSize, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
    }

    @Override
    protected void initializeMonitoring() {
    }

    @Override
    protected void postMessage(DataBucket message) {

    }

    @Override
    protected void deinitializeMonitoring() {
        if (monitorTask != null) {
            monitorTask.cancel();
        }
    }

    @Override
    protected void preProcessFrame(ByteBuffer frame) {
    }

    @Override
    protected void postProcessFrame(long startTime, ByteBuffer frame) {

    }

}
