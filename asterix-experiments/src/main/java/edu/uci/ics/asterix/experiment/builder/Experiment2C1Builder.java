package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2C1Builder extends AbstractExperiment2CBuilder {

    public Experiment2C1Builder(LSMExperimentSetRunnerConfig config) {
        super("2C1", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }

}
