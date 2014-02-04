package edu.uci.ics.asterix.experiment.client;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.experiment.builder.Experiment;
import edu.uci.ics.asterix.experiment.builder.Experiment1Builder;

public class LSMExperimentSetRunner {
    public static class LSMExperimentSetRunnerConfig {
        @Option(name = "-rh", aliases = "--rest-host", usage = "Asterix REST API host address", required = true, metaVar = "HOST")
        private String restHost;

        public String getRESTHost() {
            return restHost;
        }

        @Option(name = "-rp", aliases = "--rest-port", usage = "Asterix REST API port", required = true, metaVar = "PORT")
        private int restPort;

        public int getRESTPort() {
            return restPort;
        }

        @Option(name = "-mh", aliases = "--managix-home", usage = "Path to MANAGIX_HOME directory", required = true, metaVar = "MGXHOME")
        private String managixHome;

        public String getManagixHome() {
            return managixHome;
        }

        @Option(name = "-ler", aliases = "--local-experiment-root", usage = "Path to the local LSM experiment root directory", required = true, metaVar = "LOCALEXPROOT")
        private String localExperimentRoot;

        public String getLocalExperimentRoot() {
            return localExperimentRoot;
        }

        @Option(name = "-u", aliases = "--username", usage = "Username to use for SSH/SCP", required = true, metaVar = "UNAME")
        private String username;

        public String getUsername() {
            return username;
        }

        @Option(name = "-k", aliases = "--key", usage = "SSH key location", metaVar = "SSHKEY")
        private String sshKeyLocation;

        public String getSSHKeyLocation() {
            return sshKeyLocation;
        }
    }

    public static void main(String[] args) throws Exception {
        LSMExperimentSetRunnerConfig config = new LSMExperimentSetRunnerConfig();
        CmdLineParser clp = new CmdLineParser(config);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            clp.printUsage(System.err);
            System.exit(1);
        }

        Experiment e1 = new Experiment1Builder(config).build();
        e1.perform();
    }
}
