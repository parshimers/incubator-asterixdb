package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.Scanner;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpMethodRetryHandler;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.NoHttpResponseException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;

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

    public static boolean confirmAction(String warning) {
        System.out.println(warning);
        System.out.print("Are you sure you want to do this? (yes/no): ");
        Scanner in = new Scanner(System.in);
        while (true) {
            try {
                String input = in.nextLine();
                if (input.equals("yes")) {
                    return true;
                } else if (input.equals("no")) {
                    return false;
                } else {
                    System.out.println("Please type yes or no");
                }
            } finally {
                in.close();
            }
        }
    }

    public static void listInstances(Configuration conf, String CONF_DIR_REL) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path instanceFolder = new Path(fs.getHomeDirectory(), CONF_DIR_REL);
        FileStatus[] instances = fs.listStatus(instanceFolder);
        if (instances.length != 0) {
            System.out.println("Existing Asterix instances: ");
        } else {
            System.out.println("No running or stopped Asterix instances exist in this cluster");
        }
        for (int i = 0; i < instances.length; i++) {
            FileStatus st = instances[i];
            String name = st.getPath().getName();
            ApplicationId lockFile = Client.getLockFile(name, conf);
            if (lockFile != null) {
                System.out.println("Instance " + name + " is running with Application ID: " + lockFile.toString());
            } else {
                System.out.println("Instance " + name + " is stopped");
            }
        }
    }

    public static void listBackups(Configuration conf, String CONF_DIR_REL, String instance) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path backupFolder = new Path(fs.getHomeDirectory(), CONF_DIR_REL + "/" + instance + "/" + "backups");
        FileStatus[] backups = fs.listStatus(backupFolder);
        if (backups.length != 0) {
            System.out.println("Backups for instance " + instance + ": ");
        }
        else{
            System.out.println("No backups found for instance " + instance + ".");
        }
        for(FileStatus f: backups){
            String name = f.getPath().getName();
            System.out.println("Backup: " + name);
        }
    }
}
