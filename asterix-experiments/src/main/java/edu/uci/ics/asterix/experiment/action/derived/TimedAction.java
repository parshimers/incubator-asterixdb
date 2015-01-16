package edu.uci.ics.asterix.experiment.action.derived;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.experiment.action.base.IAction;

public class TimedAction extends AbstractAction {

    private final Logger LOGGER = Logger.getLogger(TimedAction.class.getName());

    private final IAction action;

    public TimedAction(IAction action) {
        this.action = action;
    }

    @Override
    protected void doPerform() throws Exception {
        long start = System.currentTimeMillis();
        action.perform();
        long end = System.currentTimeMillis();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Elapsed time = " + (end - start) + " for action " + action);
        }
    }
}
