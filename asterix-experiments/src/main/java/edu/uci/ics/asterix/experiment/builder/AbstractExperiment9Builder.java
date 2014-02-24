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
import edu.uci.ics.asterix.experiment.action.base.IAction;
import edu.uci.ics.asterix.experiment.action.base.ParallelActionSet;
import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.AbstractRemoteExecutableAction;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLStringAction;
import edu.uci.ics.asterix.experiment.action.derived.TimedAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;
import edu.uci.ics.asterix.experiment.client.OrchestratorServer9;
import edu.uci.ics.asterix.experiment.client.OrchestratorServer9.IProtocolActionBuilder;

public abstract class AbstractExperiment9Builder extends AbstractLSMBaseExperimentBuilder {

    private static final long DOMAIN_SIZE = (1L << 32);

    private static final long EXPECTED_RANGE_CARDINALITY = 1000;

    private static int N_PARTITIONS = 16;

    private final int nIntervals;

    private final String orchHost;

    private final int orchPort;

    protected final long dataInterval;

    protected final int nQueryRuns;

    protected final Random randGen;

    public AbstractExperiment9Builder(String name, LSMExperimentSetRunnerConfig config, String clusterConfigFileName,
            String ingestFileName, String dgenFileName) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName, null);
        nIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        dataInterval = config.getDataInterval();
        this.nQueryRuns = config.getNQueryRuns();
        this.randGen = new Random();
    }

    @Override
    protected void doBuildDataGen(SequentialActionList seq, Map<String, List<String>> dgenPairs) throws Exception {
        int nDgens = 0;
        for (List<String> v : dgenPairs.values()) {
            nDgens += v.size();
        }
        final OrchestratorServer9 oServer = new OrchestratorServer9(orchPort, nDgens, nIntervals,
                new ProtocolActionBuilder());

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

    public class ProtocolActionBuilder implements IProtocolActionBuilder {

        private IAction lastAction;

        private int lastRound;

        private final String pointQueryTemplate;

        private final String rangeQueryTemplate;

        public ProtocolActionBuilder() {
            lastAction = null;
            lastRound = -1;
            this.pointQueryTemplate = getPointQueryTemplate();
            this.rangeQueryTemplate = getRangeQueryTemplate();
        }

        private String getRangeQueryTemplate() {
            try {
                Path aqlTemplateFilePath = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(
                        "8_q2.aql");
                return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlTemplateFilePath)))
                        .toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private String getPointQueryTemplate() {
            try {
                Path aqlTemplateFilePath = localExperimentRoot.resolve(LSMExperimentConstants.AQL_DIR).resolve(
                        "8_q1.aql");
                return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlTemplateFilePath)))
                        .toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public IAction buildAction(int round) throws Exception {
            if (round == lastRound) {
                return lastAction;
            }
            SequentialActionList protoAction = new SequentialActionList();
            IAction pointQueryAction = new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort,
                    getPointLookUpAQL(round)));
            IAction rangeQueryAction = new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort,
                    getRangeAQL(round)));
            protoAction.add(pointQueryAction);
            protoAction.add(rangeQueryAction);
            lastRound = round;
            lastAction = protoAction;
            return protoAction;
        }

        private String getPointLookUpAQL(int round) throws Exception {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.put((byte) 0);
            bb.put((byte) randGen.nextInt(N_PARTITIONS));
            bb.putShort((short) 0);
            bb.putInt(randGen.nextInt((int) (((1 + round) * dataInterval) / 1000)));
            bb.flip();
            long key = bb.getLong();
            return pointQueryTemplate.replaceAll("\\$KEY\\$", Long.toString(key));
        }

        private String getRangeAQL(int round) throws Exception {
            long numKeys = (((1 + round) * dataInterval) / 1000) * N_PARTITIONS;
            long rangeSize = (long) ((EXPECTED_RANGE_CARDINALITY / (double) numKeys) * DOMAIN_SIZE);
            int lowKey = randGen.nextInt();
            long maxLowKey = Integer.MAX_VALUE - rangeSize;
            if (lowKey > maxLowKey) {
                lowKey = (int) maxLowKey;
            }
            int highKey = (int) (lowKey + rangeSize);
            return rangeQueryTemplate.replaceAll("\\$LKEY\\$", Long.toString(lowKey)).replaceAll("\\$HKEY\\$",
                    Long.toString(highKey));
        }

    }
}
