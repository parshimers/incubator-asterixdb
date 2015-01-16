package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public class Experiment2A4Builder extends AbstractExperiment2ABuilder {

    public Experiment2A4Builder(LSMExperimentSetRunnerConfig config) {
        super("2A4", config, "4node.xml", "base_4_ingest.aql", "4.dgen");
    }

}
