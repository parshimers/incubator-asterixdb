package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2D4Builder extends AbstractExperiment2DBuilder {

    public Experiment2D4Builder(LSMExperimentSetRunnerConfig config) {
        super("2D4", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
