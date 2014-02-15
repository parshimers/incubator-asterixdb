package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2D2Builder extends AbstractExperiment2DBuilder {

    public Experiment2D2Builder(LSMExperimentSetRunnerConfig config) {
        super("2D2", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
