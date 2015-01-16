package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment5CBuilder extends AbstractLSMBaseExperimentBuilder {

    public Experiment5CBuilder(LSMExperimentSetRunnerConfig config) {
        super("5C", config, "8node.xml", "5_3_ingest.aql", "5_3.dgen", "5_3_count.aql");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("5_3.aql")));
    }

}
