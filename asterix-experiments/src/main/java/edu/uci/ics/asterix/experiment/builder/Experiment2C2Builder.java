package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2C2Builder extends AbstractExperiment2CBuilder {

    public Experiment2C2Builder(LSMExperimentSetRunnerConfig config) {
        super("2C2", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
