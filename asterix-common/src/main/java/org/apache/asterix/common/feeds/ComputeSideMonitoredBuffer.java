package org.apache.asterix.common.feeds;

import org.apache.asterix.common.feeds.api.IExceptionHandler;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFrameEventCallback;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class ComputeSideMonitoredBuffer extends MonitoredBuffer {

    public ComputeSideMonitoredBuffer(IHyracksTaskContext ctx, FeedRuntimeInputHandler inputHandler, IFrameWriter frameWriter,
            FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedMetricCollector metricCollector,
            FeedConnectionId connectionId, FeedRuntimeId runtimeId, IExceptionHandler exceptionHandler,
            IFrameEventCallback callback, int nPartitions, FeedPolicyAccessor policyAccessor) {
        super(ctx, inputHandler, frameWriter, fta, recordDesc, metricCollector, connectionId, runtimeId,
                exceptionHandler, callback, nPartitions, policyAccessor);
    }

    @Override
    protected boolean monitorProcessingRate() {
        return true;
    }

    protected boolean logInflowOutflowRate() {
        return true;
    }

    @Override
    protected boolean monitorInputQueueLength() {
        return true;
    }

    @Override
    protected IFramePreprocessor getFramePreProcessor() {
        return null;
    }

    @Override
    protected IFramePostProcessor getFramePostProcessor() {
        return null;
    }

    @Override
    protected boolean reportOutflowRate() {
        return false;
    }

    @Override
    protected boolean reportInflowRate() {
        return false;
    }

}
