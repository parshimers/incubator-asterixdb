package edu.uci.ics.asterix.experiment.builder;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.asterix.experiment.action.base.SequentialActionList;
import edu.uci.ics.asterix.experiment.action.derived.RunAQLFileAction;
import edu.uci.ics.asterix.experiment.client.SocketDataGeneratorExecutable;

public class SingleNodeIngestionExperimentBuilder extends AbstractLocalExperimentBuilder {

    private final String adapterHost;

    private final int adapterPort;

    private final HttpClient httpClient;

    private final String restHost;

    private final int restPort;

    private final Path aqlFilePath;

    public SingleNodeIngestionExperimentBuilder(String adapterHost, int adapterPort, String restHost, int restPort,
            String aqlFilePath) {
        super("Local Ingestion Experiment", 2);
        this.adapterHost = adapterHost;
        this.adapterPort = adapterPort;
        httpClient = new DefaultHttpClient();
        this.restHost = restHost;
        this.restPort = restPort;
        this.aqlFilePath = Paths.get(aqlFilePath);
    }

    @Override
    protected void addPre(SequentialActionList pre) {
        pre.add(new RunAQLFileAction(httpClient, restHost, restPort, aqlFilePath));
    }

    @Override
    protected void addPost(SequentialActionList post) {

    }

    @Override
    protected void doBuild(Experiment e) {
        e.addBody(new SocketDataGeneratorExecutable(adapterHost, adapterPort));
    }
}
