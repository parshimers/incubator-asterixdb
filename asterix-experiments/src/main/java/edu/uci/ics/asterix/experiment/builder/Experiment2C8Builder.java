package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2C8Builder extends AbstractExperiment2CBuilder {

    public Experiment2C8Builder(LSMExperimentSetRunnerConfig config) {
        super("2C8", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

}
