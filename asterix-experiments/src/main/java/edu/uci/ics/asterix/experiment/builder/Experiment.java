package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.experiment.action.base.IAction;


public class Experiment extends AbstractAction {
    private final String name;

    private IAction body;

    public Experiment(String name) {
        this.name = name;
    }

    public void addBody(IAction exec) {
        body = exec;
    }

    @Override
    protected void doPerform() throws Exception {
        if (body != null) {
            body.perform();
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
