package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment5DBuilder extends AbstractLSMBaseExperimentBuilder {

    public Experiment5DBuilder(LSMExperimentSetRunnerConfig config) {
        super("5D", config, "8node.xml", "5_4_ingest.aql", "5_4.dgen", "5_4_count.aql");
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("5_4.aql")));
    }

}
