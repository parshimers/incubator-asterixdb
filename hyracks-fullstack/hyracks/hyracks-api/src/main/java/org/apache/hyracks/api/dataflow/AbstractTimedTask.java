package org.apache.hyracks.api.dataflow;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public abstract class AbstractTimedTask implements IPassableTimer {

    protected long frameStart = 0;
    protected final ICounter counter;
    protected final IStatsCollector collector;
    protected final ActivityId root;

    public AbstractTimedTask(IStatsCollector collector, ICounter counter,
                            ActivityId root) {
        this.collector = collector;
        this.counter = counter;
        this.root = root;
    }

    protected void stopClock() {
        pause();
        collector.giveClock(this, root);
    }

    protected void startClock() {
        if (frameStart > 0) {
            return;
        }
        frameStart = collector.takeClock(this, root);
    }

    @Override
    public void resume() {
        if (frameStart > 0) {
            return;
        }
        long nt = System.nanoTime();
        frameStart = nt;
    }

    @Override
    public void pause() {
        if (frameStart > 1) {
            long nt = System.nanoTime();
            long delta = nt - frameStart;
            counter.update(delta);
            frameStart = -1;
        }
    }
}
