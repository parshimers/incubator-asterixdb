package edu.uci.ics.asterix.experiment.builder;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.client.LSMExperimentConstants;
import edu.uci.ics.asterix.experiment.client.LSMExperimentSetRunner.LSMExperimentSetRunnerConfig;

public abstract class AbstractExperiment1Builder extends AbstractLSMBaseExperimentBuilder {

    public AbstractExperiment1Builder(String name, LSMExperimentSetRunnerConfig config, String clusterConfigFileName,
            String ingestFileName, String dgenFileName) {
        super(name, config, clusterConfigFileName, ingestFileName, dgenFileName);
    }

    @Override
    protected void doBuildDDL(SequentialActionList seq) {
        seq.add(new RunAQLFileAction(httpClient, restHost, restPort, localExperimentRoot.resolve(
                LSMExperimentConstants.AQL_DIR).resolve("1.aql")));
    }

}
