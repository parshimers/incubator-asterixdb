package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.action.derived.TimedAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment7CBuilder extends AbstractExperiment7Builder {

    public Experiment7CBuilder(LSMExperimentSetRunnerConfig config) {
        super("7C", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("7_c.aql")));
    }

    @Override
    protected void doBuildPauseAction(SequentialActionList seq) throws Exception {
        seq.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("7_compact.aql"))));
        for (int i = 0; i < nQueryRuns; ++i) {
            seq.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve("7_q1.aql"))));
        }
        //        seq.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
        //                LSMExperimentConstants.AQL_DIR).resolve("count.aql"))));
        for (int i = 0; i < nQueryRuns; ++i) {
            seq.add(new TimedAction(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                    LSMExperimentConstants.AQL_DIR).resolve("7_q2.aql"))));
        }
    }
}
