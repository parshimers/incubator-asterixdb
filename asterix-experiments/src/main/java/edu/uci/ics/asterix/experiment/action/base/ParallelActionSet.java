package edu.uci.ics.asterix.experiment.action.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ParallelActionSet extends AbstractAction {

    private final ExecutorService executor;

    private final List<IAction> actions;

    public ParallelActionSet() {
        executor = Executors.newCachedThreadPool();
        actions = new ArrayList<>();
    }

    public void add(IAction action) {
        actions.add(action);
    }

    @Override
    protected void doPerform() throws Exception {
        for (final IAction a : actions) {
            executor.execute(new Runnable() {

                @Override
                public void run() {
                    a.perform();
                }
            });
        }
    }

}
