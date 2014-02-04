package edu.uci.ics.asterix.experiment.action.base;

import java.util.ArrayList;
import java.util.List;

public class SequentialActionList extends AbstractAction {
    private final List<IAction> actions;

    public SequentialActionList() {
        actions = new ArrayList<>();
    }

    public void add(IAction exec) {
        actions.add(exec);
    }

    @Override
    protected void doPerform() throws Exception {
        for (IAction e : actions) {
            e.perform();
        }
    }
}
