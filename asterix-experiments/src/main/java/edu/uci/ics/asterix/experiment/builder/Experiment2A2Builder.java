package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2A2Builder extends AbstractExperiment2ABuilder {

    public Experiment2A2Builder(LSMExperimentSetRunnerConfig config) {
        super("2A2", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
