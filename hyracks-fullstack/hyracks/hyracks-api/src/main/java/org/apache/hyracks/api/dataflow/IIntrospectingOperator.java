package org.apache.hyracks.api.dataflow;

import org.apache.hyracks.api.job.profiling.IOperatorStats;

public interface IIntrospectingOperator {
    void setOperatorStats(IOperatorStats stats);
}
