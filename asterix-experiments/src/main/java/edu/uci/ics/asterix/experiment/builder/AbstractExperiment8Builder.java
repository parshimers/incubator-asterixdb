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
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import edu.uci.ics.asterix.experiment.client.OrchestratorServer;

public abstract class AbstractExperiment8Builder extends AbstractLSMBaseExperimentBuilder {

    private static final long DOMAIN_SIZE = (1L << 32);

    private static final long EXPECTED_RANGE_CARDINALITY = 1000;

    private static int N_PARTITIONS = 16;

    private final int nIntervals;

    private final String orchHost;

    private final int orchPort;

    protected final long dataInterval;

    protected final int nQueryRuns;

    protected final Random randGen;

    public AbstractExperiment8Builder(String name, LSMExperimentSetRunnerConfig config, String clusterConfigFileName,
            String ingestFileName, String dgenFileName) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName, null);
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
        for (int i = 0; i < nIntervals; i++) {
            protocolActions[i] = new SequentialActionList();
            doBuildProtocolAction(protocolActions[i], i);
        }

        int nDgens = 0;
        for (List<String> v : dgenPairs.values()) {
            nDgens += v.size();
        }
        final OrchestratorServer oServer = new OrchestratorServer(orchPort, nDgens, nIntervals, protocolActions);

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
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.put((byte) 0);
        bb.put((byte) randGen.nextInt(N_PARTITIONS));
        bb.putShort((short) 0);
        bb.putInt(randGen.nextInt((int) (((1 + round) * dataInterval) / 1000)));
        bb.flip();
        long key = bb.getLong();
        return aqlTemplate.replaceAll("\\$KEY\\$", Long.toString(key));
    }

    protected String getRangeAQL(String templateFileName, int round) throws Exception {
        Path aqlTemplateFilePath = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR)
                .resolve(templateFileName);
        String aqlTemplate = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlTemplateFilePath)))
                .toString();
        long numKeys = (((1 + round) * dataInterval) / 1000) * N_PARTITIONS;
        long rangeSize = (long) ((EXPECTED_RANGE_CARDINALITY / (double) numKeys) * DOMAIN_SIZE);
        int lowKey = randGen.nextInt();
        long maxLowKey = Integer.MAX_VALUE - rangeSize;
        if (lowKey > maxLowKey) {
            lowKey = (int) maxLowKey;
        }
        int highKey = (int) (lowKey + rangeSize);
        return aqlTemplate.replaceAll("\\$LKEY\\$", Long.toString(lowKey)).replaceAll("\\$HKEY\\$",
                Long.toString(highKey));
    }
}
