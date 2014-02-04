package edu.uci.ics.asterix.experiment.action.derived;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.xfer.scp.SCPFileTransfer;
import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public class RemoteCopyFileAction extends AbstractAction {

    private final String srcFilePath;

    private final String destFilePath;

    private final SSHClient client;

    private final String hostname;

    private final int port;

    private final String username;

    private final String keyLocation;

    public RemoteCopyFileAction(String srcFilePath, String destFilePath, String hostname, String username,
            String keyLocation) {
        this(srcFilePath, destFilePath, hostname, SSHClient.DEFAULT_PORT, username, keyLocation);
    }

    public RemoteCopyFileAction(String srcFilePath, String destFilePath, String hostname, int port, String username,
            String keyLocation) {
        this.srcFilePath = srcFilePath;
        this.destFilePath = destFilePath;
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.keyLocation = keyLocation;
        client = new SSHClient();
    }

    @Override
    protected void doPerform() throws Exception {
        client.loadKnownHosts();
        try {
            client.connect(hostname, port);
            client.authPublickey(username, keyLocation);
            SCPFileTransfer scpft = client.newSCPFileTransfer();
            scpft.upload(srcFilePath, destFilePath);
        } finally {
            client.close();
        }
    }
}
