package org.apache.hyracks.api.dataflow;

import org.apache.hyracks.api.job.profiling.IOperatorStats;

public interface IMetaOperator extends IIntrospectingOperator{
    void setParentStats(IOperatorStats parentStats);
    void setAcId(ActivityId acId);

}
