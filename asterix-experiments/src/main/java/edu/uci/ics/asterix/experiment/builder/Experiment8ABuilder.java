package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLStringAction;
import edu.uci.ics.asterix.experiment.action.derived.TimedAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment8ABuilder extends AbstractExperiment8Builder {

    public Experiment8ABuilder(LSMExperimentSetRunnerConfig config) {
        super("8A", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("8_a.aql")));
    }

    @Override
    protected void doBuildProtocolAction(SequentialActionList seq, int round) throws Exception {
        for (int i = 0; i < nQueryRuns; ++i) {
            String aql = getPointLookUpAQL("8_q1.aql", round);
            seq.add(new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort, aql)));

            aql = getRangeAQL("8_q2.aql", round);
            seq.add(new TimedAction(new RunAQLStringAction(httpClient, restHost, restPort, aql)));
        }
    }
}
