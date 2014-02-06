package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2B2Builder extends AbstractExperiment2BBuilder {

    public Experiment2B2Builder(LSMExperimentSetRunnerConfig config) {
        super("2B2", config, "2node.xml", "base_2_ingest.aql", "2.dgen");
    }

}
