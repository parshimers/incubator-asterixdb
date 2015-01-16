package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment3ABuilder extends AbstractExperiment3Builder {

    public Experiment3ABuilder(LSMExperimentSetRunnerConfig config) {
        super("3A", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }

}
