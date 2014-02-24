package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment5BBuilder extends AbstractLSMBaseExperimentBuilder {

    public Experiment5BBuilder(LSMExperimentSetRunnerConfig config) {
        super("5B", config, "8node.xml", "5_2_ingest.aql", "5_2.dgen", "5_2_count.aql");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("5_2.aql")));
    }

}
