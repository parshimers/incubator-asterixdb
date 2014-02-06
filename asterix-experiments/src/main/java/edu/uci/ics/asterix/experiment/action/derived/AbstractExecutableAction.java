package edu.uci.ics.asterix.experiment.action.derived;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public abstract class AbstractExecutableAction extends AbstractAction {

    private static final Logger LOGGER = Logger.getLogger(AbstractExecutableAction.class.getName());

    protected Map<String, String> getEnvironment() {
        return Collections.<String, String> emptyMap();
    }

    protected abstract String getCommand();

    protected abstract boolean doExecute(String command, Map<String, String> env) throws Exception;

    protected abstract InputStream getErrorStream();

    protected abstract InputStream getInputStream();

    @Override
    protected void doPerform() throws Exception {
        StringWriter sw = new StringWriter();
        String cmd = getCommand();
        if (!doExecute(cmd, getEnvironment())) {
            IOUtils.copy(getErrorStream(), sw);
            throw new AsterixException("Error executing command: " + cmd + ".\n Error = " + sw.toString());
        } else {
            IOUtils.copy(getInputStream(), sw);
            IOUtils.copy(getErrorStream(), sw);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(sw.toString());
        }
    }
}
