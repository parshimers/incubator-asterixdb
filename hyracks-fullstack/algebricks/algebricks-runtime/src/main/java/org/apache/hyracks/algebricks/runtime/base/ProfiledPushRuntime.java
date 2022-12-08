package org.apache.hyracks.algebricks.runtime.base;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.ProfiledFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;

public class ProfiledPushRuntime extends ProfiledFrameWriter implements IPushRuntime {

    private final IPushRuntime pr;

    public ProfiledPushRuntime(IPushRuntime pushRuntime, IStatsCollector collector, String name, IOperatorStats stats,
            IOperatorStats parentStats) {
        super(pushRuntime, collector, name, stats, parentStats);
        this.pr = pushRuntime;
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        pr.setOutputFrameWriter(index, writer, recordDesc);
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        pr.setInputRecordDescriptor(index, recordDescriptor);
    }
}
