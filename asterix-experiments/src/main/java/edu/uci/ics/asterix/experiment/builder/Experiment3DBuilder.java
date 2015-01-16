package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment3DBuilder extends AbstractExperiment3Builder {

    public Experiment3DBuilder(LSMExperimentSetRunnerConfig config) {
        super("3D", config, "8node.xml", "base_8_ingest.aql", "8.dgen");
    }

}
