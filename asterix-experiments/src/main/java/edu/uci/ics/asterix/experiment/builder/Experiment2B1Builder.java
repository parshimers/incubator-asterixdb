package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2B1Builder extends AbstractExperiment2BBuilder {

    public Experiment2B1Builder(LSMExperimentSetRunnerConfig config) {
        super("2B1", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }
}
