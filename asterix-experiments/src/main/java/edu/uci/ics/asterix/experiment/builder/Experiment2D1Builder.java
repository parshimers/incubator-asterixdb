package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2D1Builder extends AbstractExperiment2DBuilder {

    public Experiment2D1Builder(LSMExperimentSetRunnerConfig config) {
        super("2D1", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }

}
