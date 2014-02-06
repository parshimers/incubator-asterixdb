package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2B8Builder extends AbstractExperiment2BBuilder {

    public Experiment2B8Builder(LSMExperimentSetRunnerConfig config) {
        super("2B8", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

}
