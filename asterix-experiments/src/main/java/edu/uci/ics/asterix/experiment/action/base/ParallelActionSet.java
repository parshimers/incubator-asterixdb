package edu.uci.ics.asterix.experiment.action.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelActionSet extends AbstractAction {

    private final ExecutorService executor;

    private final List<IAction> actions;

    public ParallelActionSet() {
        executor = Executors.newCachedThreadPool(new ThreadFactory() {

            private final AtomicInteger tid = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("ParallelActionThread " + tid.getAndIncrement());
                return t;
            }
        });
        actions = new ArrayList<>();
    }

    public void add(IAction action) {
        actions.add(action);
    }

    @Override
    protected void doPerform() throws Exception {
        final Semaphore sem = new Semaphore(-(actions.size() - 1));
        for (final IAction a : actions) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        a.perform();
                    } finally {
                        sem.release();
                    }
                }
            });
        }
        sem.acquire();
    }

}
