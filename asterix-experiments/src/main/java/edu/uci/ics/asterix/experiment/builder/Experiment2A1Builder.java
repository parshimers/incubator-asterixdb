package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2A1Builder extends AbstractExperiment2ABuilder {

    public Experiment2A1Builder(LSMExperimentSetRunnerConfig config) {
        super("2A1", config, "1node.xml", "base_1_ingest.aql", "1.dgen");
    }

}
