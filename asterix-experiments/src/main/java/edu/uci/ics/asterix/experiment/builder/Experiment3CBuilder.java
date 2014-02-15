package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment3CBuilder extends AbstractExperiment3Builder {

    public Experiment3CBuilder(LSMExperimentSetRunnerConfig config) {
        super("3C", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
