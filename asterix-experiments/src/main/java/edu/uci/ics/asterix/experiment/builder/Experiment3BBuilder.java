package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment3BBuilder extends AbstractExperiment3Builder {

    public Experiment3BBuilder(LSMExperimentSetRunnerConfig config) {
        super("3B", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
