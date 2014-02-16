package edu.uci.ics.asterix.experiment.builder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.experiment.action.base.ParallelActionSet;
import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.OrchestratorServer;

public abstract class AbstractExperiment8Builder extends AbstractLSMBaseExperimentBuilder {

    private final int nIntervals;

    private final String orchHost;

    private final int orchPort;

    protected final long dataInterval;

    protected final int nQueryRuns;
    
    protected final Random randGen;
    
    public AbstractExperiment8Builder(String name, LSMExperimentSetRunnerConfig config, String clusterConfigFileName,
            String ingestFileName, String dgenFileName) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName, null, false);
        nIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        dataInterval = config.getDataInterval();
        this.nQueryRuns = config.getNQueryRuns();
        this.randGen = new Random();
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
                    String binary = "JAVA_HOME=/home/zheilbro/java/jdk1.7.0_51 "
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

    protected String getPointLookUpAQL(String templateFileName, int round) throws Exception {
        Path aqlTemplateFilePath = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                .resolve(templateFileName);
        String aqlTemplate = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlTemplateFilePath)))
                .toString();
        long key = randGen.nextLong() % ((round * dataInterval) / 1000);
        return aqlTemplate.replaceAll("\\$KEY\\$", Long.toString(key));
    }
    
    protected String getRangeAQL(String templateFileName, int round) throws Exception {
        Path aqlTemplateFilePath = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                .resolve(templateFileName);
        String aqlTemplate = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlTemplateFilePath)))
                .toString();
        long maxKey = (round * dataInterval) / 1000;  
        long lowKey = randGen.nextLong() % maxKey;
        if (lowKey+1000 > maxKey) {
            lowKey = lowKey - 1000;
        }
        long highKey = lowKey + 1000;
        return aqlTemplate.replaceAll("\\$LKEY\\$", Long.toString(lowKey)).replaceAll("\\$HKEY\\$", Long.toString(highKey));
    }
}
