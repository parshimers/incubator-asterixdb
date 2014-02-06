package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment1ABuilder extends AbstractExperiment1Builder {

    public Experiment1ABuilder(LSMExperimentSetRunnerConfig config) {
        super("1A", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }

}
