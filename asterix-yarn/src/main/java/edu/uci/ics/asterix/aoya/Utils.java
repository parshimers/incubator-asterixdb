package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.util.Scanner;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
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
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

public class Utils {

    private static final String CONF_DIR_REL = AsterixYARNClient.CONF_DIR_REL;

    public static String hostFromContainerID(String containerID) {
        return containerID.split("_")[4];
    }

    /**
     * Gets the metadata node from an AsterixDB cluster description file
     * 
     * @param cluster
     *            The cluster description in question.
     * @return
     */
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

    /**
     * Sends a "poison pill" message to an AsterixDB instance for it to shut down safely.
     * 
     * @param host
     *            The host to shut down.
     * @throws IOException
     */

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
        throw new IOException("Instance did not shut down cleanly.");
    }

    /**
     * Simple test via the AsterixDB Javascript API to determine if an instance is truly live or not.
     * Runs the query "1+1" and returns true if the query completes successfully, false otherwise.
     * 
     * @param host
     *            The host to run the query against
     * @return
     *         True if the instance is OK, false otherwise.
     * @throws IOException
     */
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

    //**

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

    /**
     * Lists the deployed instances of AsterixDB on a YARN cluster
     * 
     * @param conf
     *            Hadoop configuration object
     * @param CONF_DIR_REL
     *            Relative AsterixDB configuration path for DFS
     * @throws IOException
     */

    public static void listInstances(Configuration conf, String CONF_DIR_REL) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path instanceFolder = new Path(fs.getHomeDirectory(), CONF_DIR_REL);
        if (!fs.exists(instanceFolder)) {
            System.out.println("No running or stopped AsterixDB instances exist in this cluster.");
            return;
        }
        FileStatus[] instances = fs.listStatus(instanceFolder);
        if (instances.length != 0) {
            System.out.println("Existing AsterixDB instances: ");
        } else {
            System.out.println("No running or stopped AsterixDB instances exist in this cluster");
        }
        for (int i = 0; i < instances.length; i++) {
            FileStatus st = instances[i];
            String name = st.getPath().getName();
            ApplicationId lockFile = AsterixYARNClient.getLockFile(name, conf);
            if (lockFile != null) {
                System.out.println("Instance " + name + " is running with Application ID: " + lockFile.toString());
            } else {
                System.out.println("Instance " + name + " is stopped");
            }
        }
    }

    /**
     * Lists the backups in the DFS.
     * 
     * @param conf
     *            YARN configuration
     * @param CONF_DIR_REL
     *            Relative config path
     * @param instance
     *            Instance name
     * @throws IOException
     */
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

    /**
     * Removes backup snapshots from the DFS
     * 
     * @param conf
     *            DFS Configuration
     * @param CONF_DIR_REL
     *            Configuration relative directory
     * @param instance
     *            The asterix instance name
     * @param timestamp
     *            The snapshot timestap (ID)
     * @throws IOException
     */
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

    public static void writeYarnClusterConfig(String path, Cluster cl) throws FileNotFoundException, JAXBException {
        File f = new File(path);
        JAXBContext configCtx = JAXBContext.newInstance(Cluster.class);
        Marshaller marhsaller = configCtx.createMarshaller();
        marhsaller.marshal(cl, f);
    }

    /**
     * Looks in the current class path for AsterixDB libraries and gets the version number from the name of the first match.
     * 
     * @return The version found, as a string.
     */

    public static String getAsterixVersionFromClasspath() {
        String[] cp = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        String asterixJarPattern = "^(asterix).*(jar)$"; //starts with asterix,ends with jar

        for (String j : cp) {
            String[] pathComponents = j.split(File.separator);
            if (pathComponents[pathComponents.length - 1].matches(asterixJarPattern)) {
                //get components of maven version
                String[] byDash = pathComponents[pathComponents.length - 1].split("-");
                //get the version number but remove the possible '.jar' tailing it
                String version = (byDash[2].split("\\."))[0];
                //SNAPSHOT suffix
                if (byDash.length == 4) {
                    //do the same if it's a snapshot suffix
                    return version + '-' + (byDash[3].split("\\."))[0];
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
            if ((st != YarnApplicationState.RUNNING || report.getProgress() != 0.5f)) {
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
                if (st == YarnApplicationState.FAILED || st == YarnApplicationState.FINISHED
                        || st == YarnApplicationState.KILLED) {
                    return false;

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

    public static AsterixConfiguration loadAsterixConfig(String path) throws IOException, JAXBException {
        File f = new File(path);
        JAXBContext configCtx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        AsterixConfiguration conf = (AsterixConfiguration) unmarshaller.unmarshal(f);
        return conf;
    }

}
