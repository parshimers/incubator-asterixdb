package edu.uci.ics.asterix.experiment.action.derived;

import java.io.StringWriter;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

import org.apache.commons.io.IOUtils;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public class StartDataGeneratorAction extends AbstractAction {

    @Override
    protected void doPerform() throws Exception {
    }

    public static void main(String[] args) throws Exception {
        SSHClient sshClient = new SSHClient();
        sshClient.loadKnownHosts();
        sshClient.connect("asterix-1.ics.uci.edu");
        sshClient.authPublickey("zheilbro", "/Users/zheilbron/.ssh/id_rsa");
        Session session = sshClient.startSession();
        Command lsCmd = session.exec("ls");
        StringWriter sw = new StringWriter();
        IOUtils.copy(lsCmd.getInputStream(), sw);
        IOUtils.copy(lsCmd.getErrorStream(), sw);
        System.out.println(sw.toString());
        session.close();
        sw.close();
        sshClient.close();
    }
}
