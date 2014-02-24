package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public abstract class AbstractExperiment6Builder extends AbstractLSMBaseExperimentBuilder {

    public AbstractExperiment6Builder(String name, LSMExperimentSetRunnerConfig config) {
        super(name, config, "8node.xml", "base_8_ingest.aql", "8.dgen", "count.aql");
    }
}
