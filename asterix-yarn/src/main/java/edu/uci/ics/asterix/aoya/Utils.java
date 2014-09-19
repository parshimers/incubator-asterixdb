package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

public class Utils {

    private static final String CONF_DIR_REL = Client.CONF_DIR_REL;

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
        } else {
            System.out.println("No backups found for instance " + instance + ".");
        }
        for (FileStatus f : backups) {
            String name = f.getPath().getName();
            System.out.println("Backup: " + name);
        }
    }

    public static void rmBackup(Configuration conf, String CONF_DIR_REL, String instance, long timestamp)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path backupFolder = new Path(fs.getHomeDirectory(), CONF_DIR_REL + "/" + instance + "/" + "backups");
        FileStatus[] backups = fs.listStatus(backupFolder);
        if (backups.length != 0) {
            System.out.println("Backups for instance " + instance + ": ");
        } else {
            System.out.println("No backups found for instance " + instance + ".");
        }
        for (FileStatus f : backups) {
            String name = f.getPath().getName();
            long file_ts = Long.parseLong(name);
            if (file_ts == timestamp) {
                System.out.println("Deleting backup " + timestamp);
                if (!fs.delete(f.getPath(), true)) {
                    System.out.println("Backup could not be deleted");
                    return;
                } else {
                    return;
                }
            }
        }
        System.out.println("No backup found with specified timestamp");

    }

    /**
     * Simply parses out the YARN cluster config and instantiates it into a nice object.
     * 
     * @return The object representing the configuration
     * @throws FileNotFoundException
     * @throws JAXBException
     */
    public static Cluster parseYarnClusterConfig(String path) throws FileNotFoundException, JAXBException {
        File f = new File(path);
        JAXBContext configCtx = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        Cluster cl = (Cluster) unmarshaller.unmarshal(f);
        return cl;
    }

    public static String getAsterixVersionFromClasspath() {
        String[] cp = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        String asterixJarPattern = "^(asterix).*(jar)$"; //starts with asterix,ends with jar
        for (String j : cp) {
            String[] pathComponents = j.split(File.separator);
            if (pathComponents[pathComponents.length - 1].matches(asterixJarPattern)) {
                String[] byDash = pathComponents[pathComponents.length - 1].split("-");
                String version = byDash[2];
                //SNAPSHOT suffix
                if (byDash.length == 4) {
                    return version + '-' + byDash[3];
                }
                //stable version
                return version;
            }
        }
        return null;
    }

    public static boolean waitForLiveness(ApplicationId appId, boolean probe, boolean print, String message,
            YarnClient yarnClient, String instanceName, Configuration conf) throws YarnException, IOException,
            JAXBException {
        ApplicationReport report = yarnClient.getApplicationReport(appId);
        YarnApplicationState st = report.getYarnApplicationState();
        for (int i = 0; i < 120; i++) {
            if (st != YarnApplicationState.RUNNING || report.getProgress() != 0.5f) {
                try {
                    report = yarnClient.getApplicationReport(appId);
                    st = report.getYarnApplicationState();
                    if (print) {
                        System.out.print(message + Utils.makeDots(i) + "\r");
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else if (probe) {
                String host = getCCHostname(instanceName, conf);
                for (int j = 0; j < 60; j++) {
                    if (!Utils.probeLiveness(host)) {
                        try {
                            if (print) {
                                System.out.print(message + Utils.makeDots(i) + "\r");
                            }
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        if (print) {
                            System.out.println("");
                        }
                        return true;
                    }
                }
            } else {
                if (print) {
                    System.out.println("");
                }
                return true;
            }
        }
        if (print) {
            System.out.println("");
        }
        return false;
    }

    public static boolean waitForLiveness(ApplicationId appId, String message, YarnClient yarnClient,
            String instanceName, Configuration conf) throws YarnException, IOException, JAXBException {
        return waitForLiveness(appId, true, true, message, yarnClient, instanceName, conf);
    }

    public static boolean waitForApplication(ApplicationId appId, YarnClient yarnClient, String message)
            throws YarnException, IOException, JAXBException {
        return waitForLiveness(appId, false, true, message, yarnClient, "", null);
    }

    public static boolean waitForApplication(ApplicationId appId, YarnClient yarnClient) throws YarnException,
            IOException, JAXBException {
        return waitForLiveness(appId, false, false, "", yarnClient, "", null);
    }

    public static String getCCHostname(String instanceName, Configuration conf) throws IOException, JAXBException {
        FileSystem fs = FileSystem.get(conf);
        String instanceFolder = instanceName + "/";
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        File tmp = File.createTempFile("cluster-config", "xml");
        tmp.deleteOnExit();
        fs.copyToLocalFile(dstConf, new Path(tmp.getPath()));
        JAXBContext clusterConf = JAXBContext.newInstance(Cluster.class);
        Unmarshaller unm = clusterConf.createUnmarshaller();
        Cluster cl = (Cluster) unm.unmarshal(tmp);
        String ccIp = cl.getMasterNode().getClientIp();
        return ccIp;
    }

    public static ContainerRequest hostToRequest(String host, int mem) throws UnknownHostException {
        InetAddress hostIp = InetAddress.getByName(host);
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(mem);
        //we dont set anything else because we don't care about that and yarn doesn't honor it yet
        String[] hosts = new String[1];
        //TODO this is silly
        hosts[0] = hostIp.getHostName();
        ContainerRequest request = new ContainerRequest(capability, hosts, null, pri, false);
        return request;
    }
}
