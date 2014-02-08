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
import edu.uci.ics.asterix.experiment.action.derived.ManagixActions.LogAsterixManagixAction;
import edu.uci.ics.asterix.experiment.action.derived.ManagixActions.StopAsterixManagixAction;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.action.derived.SleepAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public abstract class AbstractLSMBaseExperimentBuilder extends AbstractExperimentBuilder {

    private static final String ASTERIX_INSTANCE_NAME = "a1";

    protected final HttpClient httpClient;

    protected final String restHost;

    protected final int restPort;

    private final String managixHomePath;

    protected final Path localExperimentRoot;

    private final String username;

    private final String sshKeyLocation;

    private final int duration;

    private final String clusterConfigFileName;

    private final String ingestFileName;

    private final String dgenFileName;

    private final String countFileName;

    public AbstractLSMBaseExperimentBuilder(String name, LSMExperimentSetRunnerConfig config,
            String clusterConfigFileName, String ingestFileName, String dgenFileName, String countFileName) {
        super(name);
        this.httpClient = new DefaultHttpClient();
        this.restHost = config.getRESTHost();
        this.restPort = config.getRESTPort();
        this.managixHomePath = config.getManagixHome();
        this.localExperimentRoot = Paths.get(config.getLocalExperimentRoot());
        this.username = config.getUsername();
        this.sshKeyLocation = config.getSSHKeyLocation();
        this.duration = config.getDuration();
        this.clusterConfigFileName = clusterConfigFileName;
        this.ingestFileName = ingestFileName;
        this.dgenFileName = dgenFileName;
        this.countFileName = countFileName;
    }

    protected abstract void doBuildDDL(SequentialActionList seq);

    @Override
    protected void doBuild(Experiment e) throws Exception {
        SequentialActionList execs = new SequentialActionList();

        String clusterConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(clusterConfigFileName).toString();
        String asterixConfigPath = localExperimentRoot.resolve(LSMExperimentConstants.CONFIG_DIR)
                .resolve(LSMExperimentConstants.ASTERIX_CONFIGURATION).toString();

        //create instance
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new DeleteAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new CreateAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, clusterConfigPath,
                asterixConfigPath));

        //run ddl statements
        execs.add(new SleepAction(4000));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_TYPES)));
        doBuildDDL(execs);
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot
                .resolve(LSMExperimentConstants.AQL_DIR).resolve(LSMExperimentConstants.BASE_DIR)
                .resolve(ingestFileName)));

        //start datagen
        final Map<String, List<String>> dgenPairs = readDatagenPairs(localExperimentRoot.resolve(
                LSMExperimentConstants.DGEN_DIR).resolve(dgenFileName));
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
                    return StringUtils.join(new String[] { binary, "-p", "" + p, "-d", "" + duration, ipPortPairs },
                            " ");
                }
            });
            partition += rcvrs.size();
        }
        execs.add(dgenActions);

        execs.add(new SleepAction(duration * 1000));
        execs.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve(countFileName)));
        execs.add(new StopAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME));
        execs.add(new LogAsterixManagixAction(managixHomePath, ASTERIX_INSTANCE_NAME, localExperimentRoot
                .resolve(LSMExperimentConstants.LOG_DIR).resolve(getName()).toString()));
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
