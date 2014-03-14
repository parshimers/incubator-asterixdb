package edu.uci.ics.asterix.experiment.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.builder.AbstractExperimentBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment1ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment1BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment1CBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment1DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment2A1Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2A2Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2A4Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2A8Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2B1Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2B2Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2B4Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2B8Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2C1Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2C2Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2C4Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2C8Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2D1Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2D2Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2D4Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment2D8Builder;
import edu.uci.ics.asterix.experiment.builder.Experiment3ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment3BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment3CBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment3DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment4ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment4BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment4CBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment4DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment5ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment5BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment5CBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment5DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment6ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment6BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment6CBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment7ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment7BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment7DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment8ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment8BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment8DBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment9ABuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment9BBuilder;
import edu.uci.ics.asterix.experiment.builder.Experiment9DBuilder;

public class LSMExperimentSetRunner {

    private static final Logger LOGGER = Logger.getLogger(LSMExperimentSetRunner.class.getName());

    public static class LSMExperimentSetRunnerConfig {

        private final String logDirSuffix;

        private final int nQueryRuns;

        public LSMExperimentSetRunnerConfig(String logDirSuffix, int nQueryRuns) {
            this.logDirSuffix = logDirSuffix;
            this.nQueryRuns = nQueryRuns;
        }

        public String getLogDirSuffix() {
            return logDirSuffix;
        }

        public int getNQueryRuns() {
            return nQueryRuns;
        }

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

        @Option(name = "-d", aliases = "--duartion", usage = "Duration in seconds", metaVar = "DURATION")
        private int duration;

        public int getDuration() {
            return duration;
        }

        @Option(name = "-regex", aliases = "--regex", usage = "Regular expression used to match experiment names", metaVar = "REGEXP")
        private String regex;

        public String getRegex() {
            return regex;
        }

        @Option(name = "-oh", aliases = "--orchestrator-host", usage = "The host address of THIS orchestrator")
        private String orchHost;

        public String getOrchestratorHost() {
            return orchHost;
        }

        @Option(name = "-op", aliases = "--orchestrator-port", usage = "The port to be used for the orchestrator server of THIS orchestrator")
        private int orchPort;

        public int getOrchestratorPort() {
            return orchPort;
        }

        @Option(name = "-di", aliases = "--data-interval", usage = " Initial data interval to use when generating data for exp 7")
        private long dataInterval;

        public long getDataInterval() {
            return dataInterval;
        }

        @Option(name = "-ni", aliases = "--num-data-intervals", usage = "Number of data intervals to use when generating data for exp 7")
        private int numIntervals;

        public int getNIntervals() {
            return numIntervals;
        }

        @Option(name = "-sf", aliases = "--stat-file", usage = "Enable IO/CPU stats and place in specified file")
        private String statFile = null;

        public String getStatFile() {
            return statFile;
        }
    }

    public static void main(String[] args) throws Exception {
        //        LogManager.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
        LSMExperimentSetRunnerConfig config = new LSMExperimentSetRunnerConfig(String.valueOf(System
                .currentTimeMillis()), 3);
        CmdLineParser clp = new CmdLineParser(config);
        try {
            clp.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            clp.printUsage(System.err);
            System.exit(1);
        }

        Collection<AbstractExperimentBuilder> suite = new ArrayList<>();
        suite.add(new Experiment7BBuilder(config));
        suite.add(new Experiment7DBuilder(config));
        suite.add(new Experiment7ABuilder(config));
        suite.add(new Experiment8DBuilder(config));
        suite.add(new Experiment8ABuilder(config));
        suite.add(new Experiment8BBuilder(config));
        suite.add(new Experiment9ABuilder(config));
        suite.add(new Experiment9DBuilder(config));
        suite.add(new Experiment9BBuilder(config));
        suite.add(new Experiment6ABuilder(config));
        suite.add(new Experiment6BBuilder(config));
        suite.add(new Experiment6CBuilder(config));
        suite.add(new Experiment2D1Builder(config));
        suite.add(new Experiment2D2Builder(config));
        suite.add(new Experiment2D4Builder(config));
        suite.add(new Experiment2D8Builder(config));
        suite.add(new Experiment2C1Builder(config));
        suite.add(new Experiment2C2Builder(config));
        suite.add(new Experiment2C4Builder(config));
        suite.add(new Experiment2C8Builder(config));
        suite.add(new Experiment2A1Builder(config));
        suite.add(new Experiment2A2Builder(config));
        suite.add(new Experiment2A4Builder(config));
        suite.add(new Experiment2A8Builder(config));
        suite.add(new Experiment2B1Builder(config));
        suite.add(new Experiment2B2Builder(config));
        suite.add(new Experiment2B4Builder(config));
        suite.add(new Experiment2B8Builder(config));
        suite.add(new Experiment1ABuilder(config));
        suite.add(new Experiment1BBuilder(config));
        suite.add(new Experiment1CBuilder(config));
        suite.add(new Experiment1DBuilder(config));
        suite.add(new Experiment4ABuilder(config));
        suite.add(new Experiment4BBuilder(config));
        suite.add(new Experiment4CBuilder(config));
        suite.add(new Experiment4DBuilder(config));
        suite.add(new Experiment3ABuilder(config));
        suite.add(new Experiment3BBuilder(config));
        suite.add(new Experiment3CBuilder(config));
        suite.add(new Experiment3DBuilder(config));
        suite.add(new Experiment5ABuilder(config));
        suite.add(new Experiment5BBuilder(config));
        suite.add(new Experiment5CBuilder(config));
        suite.add(new Experiment5DBuilder(config));

        Pattern p = config.getRegex() == null ? null : Pattern.compile(config.getRegex());

        SequentialActionList exps = new SequentialActionList();
        for (AbstractExperimentBuilder eb : suite) {
            if (p == null || p.matcher(eb.getName()).matches()) {
                exps.add(eb.build());
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Added " + eb.getName() + " to run list...");
                }
            }
        }
        exps.perform();
    }
}
