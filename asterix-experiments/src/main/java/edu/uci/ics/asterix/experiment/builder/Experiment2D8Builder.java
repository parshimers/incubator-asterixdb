package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2D8Builder extends AbstractExperiment2DBuilder {

    public Experiment2D8Builder(LSMExperimentSetRunnerConfig config) {
        super("2D8", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

}
