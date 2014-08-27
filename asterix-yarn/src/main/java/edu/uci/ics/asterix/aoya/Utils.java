package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

public class Utils {

    public static String hostFromContainerID(String containerID) {
        return containerID.split("_")[4];
    }

    public static Node getMetadataNode(Cluster cluster) {
        Node metadataNode = null;
        if (cluster.getMetadataNode() != null) {
            for (Node node : cluster.getNode()) {
                if (node.getId().equals(cluster.getMetadataNode())) {
                    metadataNode = node;
                    break;
                }
            }
        } else {
            //I will pick one for you.
            metadataNode = cluster.getNode().get(1);
        }
        return metadataNode;
    }

    public static void sendShutdownCall(String host) throws IOException {
        final String url = "http://" + host + ":19002/admin/shutdown";
        PostMethod method = new PostMethod(url);
        try {
            executeHTTPCall(method, 1);
        } catch (NoHttpResponseException e) {
            //do nothing... this is expected
        }
        //now let's test that the instance is really down, or throw an exception
        try {
            executeHTTPCall(method, 1);
        } catch (ConnectException e) {
            return;
        }
        throw new IOException("Instance did not shut down- you may need to run kill");
    }

    public static boolean probeLiveness(String host) throws IOException {
        final String url = "http://" + host + ":19002/query";
        final String test = "1+1";
        GetMethod method = new GetMethod(url);
        method.setQueryString(new NameValuePair[] { new NameValuePair("query", test) });
        int status = 0;
        InputStream response = executeHTTPCall(method, status);
        BufferedReader br = new BufferedReader(new InputStreamReader(response));
        String result = br.readLine();
        if (result == null) {
            return false;
        }
        return true;
    }

    private static InputStream executeHTTPCall(HttpMethod method, Integer status) throws HttpException, IOException {
        HttpClient client = new HttpClient();
        HttpMethodRetryHandler noop = new HttpMethodRetryHandler() {
            @Override
            public boolean retryMethod(final HttpMethod method, final IOException exception, int executionCount) {
                return false;
            }
        };
        client.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, noop);
        status = client.executeMethod(method);
        return method.getResponseBodyAsStream();
    }

    public static String makeDots(int iter) {
        int pos = iter % 3;
        char[] dots = { ' ', ' ', ' ' };
        dots[pos] = '.';
        return new String(dots);
    }
}
