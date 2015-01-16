package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2C4Builder extends AbstractExperiment2CBuilder {

    public Experiment2C4Builder(LSMExperimentSetRunnerConfig config) {
        super("2C4", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
