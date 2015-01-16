package edu.uci.ics.asterix.experiment.action.derived;

import java.text.MessageFormat;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public class RunRESTIOWaitAction extends AbstractAction {
    private static final String REST_URI_TEMPLATE = "http://{0}:{1}/iowait";

    private final HttpClient httpClient;

    private final String restHost;

    private final int restPort;

    public RunRESTIOWaitAction(HttpClient httpClient, String restHost, int restPort) {
        this.httpClient = httpClient;
        this.restHost = restHost;
        this.restPort = restPort;
    }

    @Override
    public void doPerform() throws Exception {
        String uri = MessageFormat.format(REST_URI_TEMPLATE, restHost, String.valueOf(restPort));
        HttpGet get = new HttpGet(uri);
        HttpEntity entity = httpClient.execute(get).getEntity();
        EntityUtils.consume(entity);
    }

}
