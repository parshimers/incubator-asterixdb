package edu.uci.ics.asterix.experiment.builder;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.experiment.action.base.ParallelActionSet;
import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import edu.uci.ics.asterix.experiment.client.OrchestratorServer;

public abstract class AbstractExperiment7Builder extends AbstractLSMBaseExperimentBuilder {

    private final int nIntervals;

    private final String orchHost;

    private final int orchPort;

    private final long dataInterval;

    protected final int nQueryRuns;

    public AbstractExperiment7Builder(String name, LSMExperimentSetRunnerConfig config, String clusterConfigFileName,
            String ingestFileName, String dgenFileName) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName, null);
        nIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        dataInterval = config.getDataInterval();
        this.nQueryRuns = config.getNQueryRuns();
    }

    protected abstract void doBuildProtocolAction(SequentialActionList seq, int queryRound) throws Exception;

    @Override
    protected void doBuildDataGen(SequentialActionList seq, Map<String, List<String>> dgenPairs) throws Exception {
        //start datagen
        SequentialActionList[] protocolActions = new SequentialActionList[nIntervals];
        for (int i=0; i < nIntervals; i++) {
            protocolActions[i] = new SequentialActionList();
            doBuildProtocolAction(protocolActions[i], i);
        }
        
        final OrchestratorServer oServer = new OrchestratorServer(orchPort, dgenPairs.size(), nIntervals, protocolActions);

        seq.add(new AbstractAction() {

            @Override
            protected void doPerform() throws Exception {
                oServer.start();
            }
        });

        ParallelActionSet dgenActions = new ParallelActionSet();
        int partition = 0;

        // run dgen
        for (String dgenHost : dgenPairs.keySet()) {
            final List<String> rcvrs = dgenPairs.get(dgenHost);
            final int p = partition;
            dgenActions.add(new AbstractRemoteExecutableAction(dgenHost, username, sshKeyLocation) {

                @Override
                protected String getCommand() {
                    String ipPortPairs = StringUtils.join(rcvrs.iterator(), " ");
                    String binary = "JAVA_HOME=/home/youngsk2/jdk1.7.0_25 "
                            + localExperimentRoot.resolve("bin").resolve("expclient").toString();
                    return StringUtils.join(new String[] { binary, "-p", "" + p, "-di", "" + dataInterval, "-ni",
                            "" + nIntervals, "-oh", orchHost, "-op", "" + orchPort, ipPortPairs }, " ");
                }
            });
            partition += rcvrs.size();
        }
        seq.add(dgenActions);

        // wait until all dgen / queries are done
        seq.add(new AbstractAction() {

            @Override
            protected void doPerform() throws Exception {
                oServer.awaitFinished();
            }
        });
    }

}
