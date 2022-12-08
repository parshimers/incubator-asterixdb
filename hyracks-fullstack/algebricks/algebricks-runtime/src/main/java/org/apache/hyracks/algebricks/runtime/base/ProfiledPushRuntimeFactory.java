package org.apache.hyracks.algebricks.runtime.base;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;

public class ProfiledPushRuntimeFactory implements IPushRuntimeFactory {
    private final IPushRuntimeFactory sourceFactory;
    private final IOperatorStats stats;
    private final IOperatorStats parentStats;

    public ProfiledPushRuntimeFactory(IPushRuntimeFactory sourceFactory, IOperatorStats stats,
            IOperatorStats parentStats) {
        this.sourceFactory = sourceFactory;
        this.stats = stats;
        this.parentStats = parentStats;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        IPushRuntime[] runtimes = sourceFactory.createPushRuntime(ctx);
        IStatsCollector statsCollector = ctx.getStatsCollector();
        for (int i = 0; i < runtimes.length; i++) {
            runtimes[i] =
                    new ProfiledPushRuntime(runtimes[i], statsCollector, sourceFactory.toString(), stats, parentStats);
        }
        return runtimes;
    }

    @Override
    public String toString() {
        return sourceFactory.toString();
    }
}
