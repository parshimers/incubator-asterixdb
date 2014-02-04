package edu.uci.ics.asterix.experiment.action.derived;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;

public class RunAQLFileAction extends AbstractAction {
    private static final String REST_URI_TEMPLATE = "http://{0}:{1}/aql";

    private final HttpClient httpClient;

    private final Path aqlFilePath;

    private final String restHost;

    private final int restPort;

    public RunAQLFileAction(HttpClient httpClient, String restHost, int restPort, Path aqlFilePath) {
        this.httpClient = httpClient;
        this.aqlFilePath = aqlFilePath;
        this.restHost = restHost;
        this.restPort = restPort;
    }

    @Override
    public void doPerform() throws Exception {
        String aql = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(aqlFilePath))).toString();
        String uri = MessageFormat.format(REST_URI_TEMPLATE, restHost, String.valueOf(restPort));
        HttpPost post = new HttpPost(uri);
        post.setEntity(new StringEntity(aql, StandardCharsets.UTF_8));
        EntityUtils.consume(httpClient.execute(post).getEntity());
    }
}
