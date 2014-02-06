package edu.uci.ics.asterix.experiment.builder;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.experiment.action.base.IAction;

public class Experiment extends AbstractAction {

    private static final Logger LOGGER = Logger.getLogger(Experiment.class.getName());

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
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Running experiment: " + name);
            }
            body.perform();
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
