package edu.uci.ics.asterix.experiment.action.derived;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public abstract class AbstractLocalExecutableAction extends AbstractExecutableAction {

    private final ProcessBuilder pb;

    private Process p;

    protected AbstractLocalExecutableAction() {
        pb = new ProcessBuilder();
    }

    protected InputStream getErrorStream() {
        return p == null ? null : p.getErrorStream();
    }

    protected InputStream getInputStream() {
        return p == null ? null : p.getInputStream();
    }

    @Override
    protected boolean doExecute(String command, Map<String, String> env) throws Exception {
        List<String> cmd = Arrays.asList(command.split(" "));
        pb.command(cmd);
        pb.environment().putAll(env);
        p = pb.start();
        int exitVal = p.waitFor();
        return exitVal == 0;
    }
}
