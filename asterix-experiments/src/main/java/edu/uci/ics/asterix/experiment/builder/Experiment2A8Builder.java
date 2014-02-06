package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2A8Builder extends AbstractExperiment2ABuilder {

    public Experiment2A8Builder(LSMExperimentSetRunnerConfig config) {
        super("2A8", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

}
