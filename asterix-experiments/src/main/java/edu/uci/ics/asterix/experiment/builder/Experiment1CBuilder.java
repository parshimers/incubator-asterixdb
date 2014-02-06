package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment1CBuilder extends AbstractExperiment1Builder {

    public Experiment1CBuilder(LSMExperimentSetRunnerConfig config) {
        super("1C", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
