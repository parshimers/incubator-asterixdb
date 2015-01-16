package edu.uci.ics.asterix.experiment.action.base;

import java.util.logging.Level;
import java.util.logging.Logger;

public class DefaultExceptionListener implements IExceptionListener {

    private static final Logger LOGGER = Logger.getLogger(DefaultExceptionListener.class.getName());

    @Override
    public void caughtException(Throwable t) {
        if (LOGGER.isLoggable(Level.SEVERE)) {
            LOGGER.severe("Caught exception: " + t);
            LOGGER.severe("Stopping...");
            t.printStackTrace();
        }
        System.exit(1);
    }
}
