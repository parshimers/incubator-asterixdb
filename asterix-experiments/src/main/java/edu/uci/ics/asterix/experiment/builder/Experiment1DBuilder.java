package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment1DBuilder extends AbstractExperiment1Builder {

    public Experiment1DBuilder(LSMExperimentSetRunnerConfig config) {
        super("1D", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }
}
