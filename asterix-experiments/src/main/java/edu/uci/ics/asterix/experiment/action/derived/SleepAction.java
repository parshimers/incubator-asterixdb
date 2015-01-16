package edu.uci.ics.asterix.experiment.action.derived;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public class SleepAction extends AbstractAction {

    private final long ms;

    public SleepAction(long ms) {
        this.ms = ms;
    }

    @Override
    protected void doPerform() throws Exception {
        Thread.sleep(ms);
    }

}
