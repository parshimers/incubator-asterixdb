package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2B4Builder extends AbstractExperiment2BBuilder {

    public Experiment2B4Builder(LSMExperimentSetRunnerConfig config) {
        super("2B4", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
