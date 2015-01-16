package edu.uci.ics.asterix.experiment.action.derived;

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

public abstract class AbstractRemoteExecutableAction extends AbstractExecutableAction {

    private final SSHClient client;

    private final String hostname;

    private final int port;

    private final String username;

    private final String keyLocation;

    private Command cmd;

    protected AbstractRemoteExecutableAction(String hostname, String username, String keyLocation) {
        this(hostname, SSHClient.DEFAULT_PORT, username, keyLocation);
    }

    protected AbstractRemoteExecutableAction(String hostname, int port, String username, String keyLocation) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.keyLocation = keyLocation;
        client = new SSHClient();
    }

    protected InputStream getErrorStream() {
        return cmd == null ? null : cmd.getErrorStream();
    }

    protected InputStream getInputStream() {
        return cmd == null ? null : cmd.getInputStream();
    }

    @Override
    protected boolean doExecute(String command, Map<String, String> env) throws Exception {
        int exitVal = 0;
        client.loadKnownHosts();
        try {
            client.connect(hostname, port);
            client.authPublickey(username, keyLocation);
            Session s = client.startSession();
            try {
                for (Entry<String, String> e : env.entrySet()) {
                    s.setEnvVar(e.getKey(), e.getValue());
                }
                cmd = s.exec(command);
                cmd.join();
                Integer ev = cmd.getExitStatus();
                exitVal = ev == null ? -1 : ev;
                cmd.close();
            } finally {
                s.close();
            }
        } finally {
            client.close();
        }
        return exitVal == 0;
    }
}
