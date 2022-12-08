package org.apache.hyracks.dataflow.std.base;

import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IMetaOperator;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.NoOpOperatorStats;

public abstract class AbstractMetaUnaryInputUnaryOutputOperatorNodePushable
        extends AbstractUnaryInputUnaryOutputOperatorNodePushable implements IMetaOperator {
    public IOperatorStats stats = NoOpOperatorStats.INSTANCE;
    public IOperatorStats parentStats = NoOpOperatorStats.INSTANCE;

    public ActivityId acId;

    @Override
    public void setOperatorStats(IOperatorStats stats) {
        this.stats = stats;
    }

    @Override
    public void setParentStats(IOperatorStats parentStats) {
        this.parentStats = parentStats;
    }

    @Override
    public IOperatorStats getStats() {
        return stats;
    }

    @Override
    public void setAcId(ActivityId acId) {
        this.acId = acId;
    }
}
