package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment1BBuilder extends AbstractExperiment1Builder {

    public Experiment1BBuilder(LSMExperimentSetRunnerConfig config) {
        super("1B", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
