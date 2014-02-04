package edu.uci.ics.asterix.experiment.builder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.asterix.experiment.action.base.ParallelActionSet;
import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import edu.uci.ics.asterix.experiment.action.derived.ManagixActions.CreateAsterixManagixAction;
import edu.uci.ics.asterix.experiment.action.derived.ManagixActions.DeleteAsterixManagixAction;
import edu.uci.ics.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.action.derived.SleepAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment1Builder extends AbstractExperimentBuilder {

    private static final String ASTERIX_INSTANCE_NAME = "a1";

    private static final String CLUSTER_CONFIG = "1node.xml";

    private final HttpClient httpClient;

    private final String restHost;

    private final int restPort;

    private final String managixHomePath;

    private final Path localExperimentRoot;

    private final String username;

    private final String sshKeyLocation;

    public Experiment1Builder(LSMExperimentSetRunnerConfig config) {
        super("Experiment 1");
        this.httpClient = new DefaultHttpClient();
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.managixHomePath = config.getManagixHome();
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.username = config.getUsername();
        this.sshKeyLocation = config.getSSHKeyLocation();
    }

    @Override
    protected void doBuild(Experiment e) throws Exception {
        SequentialActionList execs = new SequentialActionList();

        String clusterConfigFileName = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(CLUSTER_CONFIG).toString();
        String asterixConfigFileName = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_CONFIGURATION).toString();

        //create instance
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new DeleteAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new CreateAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, clusterConfigFileName,
                asterixConfigFileName));

        //run ddl statements
        execs.add(new SleepAction(4000));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_TYPES)));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("1.aql")));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot
                .resolve(LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_DIR)
                .resolve("base_1_ingest.aql")));

        //start datagen
        final Map<String, List<String>> dgenPairs = readDatagenPairs(localExperimentRoot.resolve(
                LSMExperimentConstants.DGEN_DIR).resolve("1.dgen"));
        ParallelActionSet dgenActions = new ParallelActionSet();
        int partition = 0;
        for (String dgenHost : dgenPairs.keySet()) {
            final List<String> rcvrs = dgenPairs.get(dgenHost);
            final int p = partition;
            dgenActions.add(new AbstractRemoteExecutableAction(dgenHost, username, sshKeyLocation) {

                @Override
                protected String getCommand() {
                    String ipPortPairs = StringUtils.join(rcvrs.iterator(), " ");
                    String binary = "JAVA_HOME=/home/zheilbro/java/jdk1.7.0_51 "
                            + localExperimentRoot.resolve("bin").resolve("expclient").toString();
                    return StringUtils.join(new String[] { binary, "-p", "" + p, "-d", "" + 10, ipPortPairs }, " ");
                }
            });
            partition += rcvrs.size();
        }
        execs.add(dgenActions);

        execs.add(new SleepAction(10000));
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));

        e.addBody(execs);
    }

    private Map<String, List<String>> readDatagenPairs(Path p) throws IOException {
        Map<String, List<String>> dgenPairs = new HashMap<>();
        Scanner s = new Scanner(p, StandardCharsets.UTF_8.name());
        try {
            while (s.hasNextLine()) {
                String line = s.nextLine();
                String[] pair = line.split("\\s+");
                List<String> vals = dgenPairs.get(pair[0]);
                if (vals == null) {
                    vals = new ArrayList<>();
                    dgenPairs.put(pair[0], vals);
                }
                vals.add(pair[1]);
            }
        } finally {
            s.close();
        }
        return dgenPairs;
    }
}
