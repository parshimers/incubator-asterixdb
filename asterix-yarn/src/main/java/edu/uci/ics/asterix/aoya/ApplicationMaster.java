/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.JAXBException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.common.configuration.Property;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.MasterNode;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
    private static final String CLUSTER_DESC_PATH = "cluster-config.xml";
    private static final String WORKING_CONF_PATH = "asterix-server.zip" + File.separator + "bin" + File.separator
            + "asterix-configuration.xml";
    private static final String ASTERIX_CC_BIN_PATH = "asterix-server.zip" + File.separator + "bin" + File.separator
            + "asterixcc";
    private static final String ASTERIX_NC_BIN_PATH = "asterix-server.zip" + File.separator + "bin" + File.separator
            + "asterixnc";

    private Map<String, Property> asterixConfigurationParams;
    private static final int CC_MEMORY_MBS_DEFAULT = 512;
    private static final int NC_MEMORY_MBS_DEFAULT = 2048;
    private static final String EXTERNAL_CC_JAVA_OPTS_KEY = "cc.java.opts";
    private static final String EXTERNAL_CC_JAVA_OPTS_DEFAULT = "-Xmx" + CC_MEMORY_MBS_DEFAULT + "m";

    private static final String EXTERNAL_NC_JAVA_OPTS_KEY = "nc.java.opts";
    private static final String EXTERNAL_NC_JAVA_OPTS_DEFAULT = "-Xmx" + NC_MEMORY_MBS_DEFAULT + "m";
    private String NcJavaOpts = EXTERNAL_NC_JAVA_OPTS_DEFAULT;
    private String CcJavaOpts = EXTERNAL_CC_JAVA_OPTS_DEFAULT;
    private static final String OBLITERATOR_CLASSNAME = "edu.uci.ics.asterix.aoya.Deleter";
    private static final String HDFS_BACKUP_CLASSNAME = "edu.uci.ics.asterix.aoya.HDFSBackup";
    private static final String LOCAL_BACKUP_CLASSNAME = "edu.uci.ics.asterix.aoya.LocalBackup";
    private boolean doneAllocating = false;

    // Configuration
    private Configuration conf;

    // Handle to communicate with the Resource Manager
    @SuppressWarnings("rawtypes")
    private AMRMClientAsync resourceManager;

    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
    // Application Attempt Id ( combination of attemptId and fail count )
    private ApplicationAttemptId appAttemptID;

    // TODO
    // For status update for clients - yet to be implemented
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = 19020;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";

    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Allocated container count so that we know how many containers has the RM
    // allocated to us
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
    // Count of containers already requested from the RM
    // Needed as once requested, we should not request for containers again.
    // Only request for more if the original requirement changes.
    private AtomicInteger numRequestedContainers = new AtomicInteger();
    //Tells us whether the Cluster Controller is up so we can safely start some Node Controllers
    private AtomicBoolean ccUp = new AtomicBoolean();
    private AtomicBoolean ccStarted = new AtomicBoolean();

    //HDFS path to Asterix distributable tarball
    private String asterixZipPath = "";
    // Timestamp needed for creating a local resource
    private long asterixZipTimestamp = 0;
    // File length needed for local resource
    private long asterixZipLen = 0;

    //HDFS path to Asterix distributable tarball
    private String asterixConfPath = "";
    // Timestamp needed for creating a local resource
    private long asterixConfTimestamp = 0;
    // File length needed for local resource
    private long asterixConfLen = 0;

    private String instanceConfPath = "";

    private int numTotalContainers = 0;

    // Set the local resources
    private Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    private Cluster clusterDesc = null;
    private MasterNode CC = null;
    private AsterixPropertiesAccessor properties;
    private volatile boolean done;
    private volatile boolean success;

    private boolean instanceExists = false;
    private boolean obliterate = false;
    private boolean refresh = false;
    private boolean backup = false;
    long backupTimestamp;
    String snapName;
    private boolean restore = false;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();
    private Thread CCThread;
    private CCApplicationContext CCContext;
    private YarnClusterLifecycleListener ccNotifications;
    private Map<String, ContainerId> nodeIdContainerMap = Collections
            .synchronizedMap(new HashMap<String, ContainerId>());

    public static void main(String[] args) {

        boolean result = false;
        try {

            LOG.debug("config file loc: " + System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY));
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            appMaster.setEnvs(appMaster.setArgs(args));
            boolean doRun = appMaster.init();
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }

    private void dumpOutDebugInfo() {

        LOG.info("Dump debug output");
        Map<String, String> envs = System.getenv();
        for (Map.Entry<String, String> env : envs.entrySet()) {
            LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
            System.out.println("System env: key=" + env.getKey() + ", val=" + env.getValue());
        }

        String cmd = "ls -alhLR";
        Runtime run = Runtime.getRuntime();
        Process pr = null;
        try {
            pr = run.exec(cmd);
            pr.waitFor();

            BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
            String line = "";
            while ((line = buf.readLine()) != null) {
                LOG.info("System CWD content: " + line);
                System.out.println("System CWD content: " + line);
            }
            buf.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ApplicationMaster() throws Exception {
        // Set up the configuration and RPC
        conf = new YarnConfiguration();
    }

    public CommandLine setArgs(String[] args) throws ParseException {
        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
        opts.addOption("existing", false, "Initialize existing Asterix instance.");
        opts.addOption("obliterate", false, "Delete asterix instance completely.");
        opts.addOption("backup", false, "Back up AsterixDB instance");
        opts.addOption("restore", true, "Restore an AsterixDB instance");
        opts.addOption("refresh", false, "Refresh all resources");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

        if (cliParser.hasOption("obliterate")) {
            obliterate = true;
        }

        if (cliParser.hasOption("refresh")) {
            refresh = true;
        }

        if (cliParser.hasOption("backup")) {
            backup = true;
            backupTimestamp = System.currentTimeMillis();
        }
        if (cliParser.hasOption("restore")) {
            restore = true;
            snapName = cliParser.getOptionValue("restore");
            LOG.info(snapName);
        }
        return cliParser;
    }

    public void setEnvs(CommandLine cliParser) {
        Map<String, String> envs = System.getenv();

        if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
            if (cliParser.hasOption("app_attempt_id")) {
                String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
                appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
            } else {
                throw new IllegalArgumentException("Application Attempt Id not set in the environment");
            }
        } else {
            ContainerId containerId = ConverterUtils.toContainerId(envs.get(Environment.CONTAINER_ID.name()));
            appAttemptID = containerId.getApplicationAttemptId();
        }

        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
        }
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, envs.get("PWD") + File.separator + "bin" + File.separator
                + "asterix-configuration.xml");

        LOG.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
                + appAttemptID.getAttemptId());

        asterixZipPath = envs.get(MConstants.TARLOCATION);
        asterixZipTimestamp = Long.parseLong(envs.get(MConstants.TARTIMESTAMP));
        asterixZipLen = Long.parseLong(envs.get(MConstants.TARLEN));

        asterixConfPath = envs.get(MConstants.CONFLOCATION);
        asterixConfTimestamp = Long.parseLong(envs.get(MConstants.CONFTIMESTAMP));
        asterixConfLen = Long.parseLong(envs.get(MConstants.CONFLEN));

        instanceConfPath = envs.get(MConstants.INSTANCESTORE);
        LOG.info("Path suffix: " + instanceConfPath);
    }

    public boolean init() throws ParseException, IOException, AsterixException, JAXBException {
        try {
            localizeDFSResources();
            clusterDesc = Utils.parseYarnClusterConfig(CLUSTER_DESC_PATH);
            CC = clusterDesc.getMasterNode();
            distributeAsterixConfig();
            //now let's read what's in there so we can set the JVM opts right
            LOG.debug("config file loc: " + System.getProperty(GlobalConfig.CONFIG_FILE_PROPERTY));
        } catch (FileNotFoundException | JAXBException | IllegalStateException e) {
            LOG.error("Could not deserialize Cluster Config from disk- aborting!");
            LOG.error(e);
            throw e;
        }

        return true;
    }

    /**
     * Kanged from managix. Splices the cluster config and asterix config parameters together, then puts the product to HDFS.
     * 
     * @param cluster
     * @throws JAXBException
     * @throws FileNotFoundException
     * @throws IOException
     */

    private void getOptsFromAsterixConfig(AsterixConfiguration configuration) {
        asterixConfigurationParams = new HashMap<String, Property>();
        for (Property p : configuration.getProperty()) {
            asterixConfigurationParams.put(p.getName(), p);
        }
        NcJavaOpts = asterixConfigurationParams.get(EXTERNAL_NC_JAVA_OPTS_KEY).getValue();
        CcJavaOpts = asterixConfigurationParams.get(EXTERNAL_CC_JAVA_OPTS_KEY).getValue();
    }

    private void distributeAsterixConfig() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = instanceConfPath + File.separator + "asterix-configuration.xml";
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        URI paramLocation = dst.toUri();
        FileStatus paramFileStatus = fs.getFileStatus(dst);
        Long paramLen = paramFileStatus.getLen();
        Long paramTimestamp = paramFileStatus.getModificationTime();
        LocalResource asterixParamLoc = Records.newRecord(LocalResource.class);
        asterixParamLoc.setType(LocalResourceType.FILE);
        asterixParamLoc.setVisibility(LocalResourceVisibility.PUBLIC);
        asterixParamLoc.setResource(ConverterUtils.getYarnUrlFromURI(paramLocation));
        asterixParamLoc.setTimestamp(paramTimestamp);
        asterixParamLoc.setSize(paramLen);
        localResources.put("asterix-configuration.xml", asterixParamLoc);

    }

    /**
     * @param c
     *            The cluster exception to attempt to alocate with the RM
     * @throws YarnException
     */
    private void requestIntialResources(Cluster c) throws YarnException, UnknownHostException {
        int numNodes = 0;
        //request NCs
        for (Node n : c.getNode()) {
            resourceManager.addContainerRequest(Utils.hostToRequest(n.getClusterIp(), NC_MEMORY_MBS_DEFAULT));
            LOG.info("Asked for NC: " + n.getClusterIp());
            numNodes++;
        }
        LOG.info("Requested all NCs and CCs. Wait for things to settle!");
        numRequestedContainers.set(numNodes);
        numTotalContainers = numNodes;
        doneAllocating = true;

    }

    /**
     * Asks the RM for a particular host, nicely.
     * 
     * @param host
     *            The host to request
     * @param cc
     *            Whether or not the host is the CC
     * @return A container request that is (hopefully) for the host we asked for.
     */

    /**
     * Writes an Asterix configuration based on the data inside the cluster description
     * 
     * @param cluster
     * @throws JAXBException
     * @throws FileNotFoundException
     */

    /**
     * Here I am just pointing the Containers to the exisiting HDFS resources given by the Client
     * filesystem of the nodes.
     * 
     * @throws IOException
     */
    private void localizeDFSResources() throws IOException {
        //if obliterating skip a lot of this.
        //FIXME
        if (obliterate || backup || restore) {
            FileSystem fs = FileSystem.get(conf);
            LocalResource obliteratorJar = Records.newRecord(LocalResource.class);
            obliteratorJar.setType(LocalResourceType.FILE);
            obliteratorJar.setVisibility(LocalResourceVisibility.APPLICATION);
            localResources.put("asterix-yarn.jar", obliteratorJar);
            LOG.info(localResources.values());
            return;
        }

        LocalResource asterixZip = Records.newRecord(LocalResource.class);

        //this un-tar's the asterix distribution
        asterixZip.setType(LocalResourceType.ARCHIVE);

        // Set visibility of the resource
        // Setting to most private option
        asterixZip.setVisibility(LocalResourceVisibility.APPLICATION);
        // Set the resource to be copied over
        try {
            asterixZip.setResource(ConverterUtils.getYarnUrlFromURI(new URI(asterixZipPath)));

        } catch (URISyntaxException e) {
            LOG.error("Error locating Asterix tarball" + " in env, path=" + asterixZipPath);
            e.printStackTrace();
        }

        asterixZip.setTimestamp(asterixZipTimestamp);
        asterixZip.setSize(asterixZipLen);
        localResources.put("asterix-server.zip", asterixZip);

        //now let's do the same for the cluster description XML
        LocalResource asterixConf = Records.newRecord(LocalResource.class);
        asterixConf.setType(LocalResourceType.FILE);

        asterixConf.setVisibility(LocalResourceVisibility.APPLICATION);
        try {
            asterixConf.setResource(ConverterUtils.getYarnUrlFromURI(new URI(asterixConfPath)));

        } catch (URISyntaxException e) {
            LOG.error("Error locating Asterix config" + " in env, path=" + asterixConfPath);
            e.printStackTrace();
        }
        //TODO: I could avoid localizing this everywhere by only calling this block on the metadata node. 
        asterixConf.setTimestamp(asterixConfTimestamp);
        asterixConf.setSize(asterixConfLen);
        localResources.put("cluster-config.xml", asterixConf);
        //now add the libraries if there are any
        try {
            FileSystem fs = FileSystem.get(conf);
            Path p = new Path(fs.getHomeDirectory(), instanceConfPath + "library/");
            if (fs.exists(p)) {
                FileStatus[] dataverses = fs.listStatus(p);
                for (FileStatus d : dataverses) {
                    if (!d.isDirectory())
                        throw new IOException("Library configuration directory structure is incorrect");
                    FileStatus[] libraries = fs.listStatus(d.getPath());
                    for (FileStatus l : libraries) {
                        if (l.isDirectory())
                            throw new IOException("Library configuration directory structure is incorrect");
                        LocalResource lr = Records.newRecord(LocalResource.class);
                        lr.setResource(ConverterUtils.getYarnUrlFromURI(l.getPath().toUri()));
                        lr.setSize(l.getLen());
                        lr.setTimestamp(l.getModificationTime());
                        lr.setType(LocalResourceType.ARCHIVE);
                        lr.setVisibility(LocalResourceVisibility.APPLICATION);
                        localResources.put("library/" + d.getPath().getName() + "/"
                                + l.getPath().getName().split("\\.")[0], lr);
                        LOG.info("Found library: " + l.getPath().toString());
                        LOG.info(l.getPath().getName());
                    }
                }
            }
        } catch (FileNotFoundException e) {
            LOG.info("No external libraries present");
            //do nothing, it just means there aren't libraries. that is possible and ok
            // it should be handled by the fs.exists(p) check though.
        }
        LOG.info(localResources.values());

    }

    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    @SuppressWarnings("unchecked")
    public void notifyDeadNode(String nodeId) throws UnknownHostException {
        ContainerId cont = nodeIdContainerMap.get(nodeId);
        nmClientAsync.stopContainerAsync(cont, containerListener.containers.get(cont).getNodeId());
        for (Node n : clusterDesc.getNode()) {
            if (n.getId().equals(nodeId)) {
                resourceManager.addContainerRequest(Utils.hostToRequest(n.getClusterIp(), NC_MEMORY_MBS_DEFAULT));
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    /**
     * Start the AM and request all necessary resources. 
     * @return True if the run fully succeeded, false otherwise. 
     * @throws YarnException
     * @throws IOException
     */
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting ApplicationMaster");

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
        resourceManager.init(conf);
        resourceManager.start();

        containerListener = new NMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        try {
            appMasterHostname = InetAddress.getLocalHost().toString();
        } catch (java.net.UnknownHostException uhe) {
            appMasterHostname = uhe.toString();
        }
        RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterHostname,
                appMasterRpcPort, appMasterTrackingUrl);

        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        try {
            String[] args = new String[] { ASTERIX_CC_BIN_PATH, "-cluster-net-ip-address",
                    InetAddress.getLocalHost().getHostAddress(), "-client-net-ip-address",
                    InetAddress.getLocalHost().getHostAddress(), "-app-cc-main-class",
                    "edu.uci.ics.asterix.hyracks.bootstrap.CCApplicationEntryPoint" };
            CCThread = new Thread(new LaunchCC(args), "CC Thread");
            ccNotifications = new YarnClusterLifecycleListener(this);
            CCThread.start();
           
            requestIntialResources(clusterDesc);
        } catch (YarnException e) {
            LOG.error("Could not allocate resources properly:" + e.getMessage());
            done = true;
            throw e;
        }
        //now we just sit and listen for messages from the RM

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        //finish();
        return success;
    }

    /**
     * Clean up, whether or not we were successful.
     */
    private void finish() {
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();

        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");

        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers + ", completed="
                    + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed="
                    + numFailedContainers.get();
            success = false;
        }
        try {
            resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        //done = true;
        resourceManager.stop();
    }

    /**
     * This handles the information that comes in from the RM while the AM
     * is running.
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        @SuppressWarnings("unchecked")
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
            for (ContainerStatus containerStatus : completedContainers) {
                LOG.info("Got container status for containerID=" + containerStatus.getContainerId() + ", state="
                        + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus()
                        + ", diagnostics=" + containerStatus.getDiagnostics());

                // non complete containers should not be here
                assert (containerStatus.getState() == ContainerState.COMPLETE);

                // increment counters for completed/failed containers
                int exitStatus = containerStatus.getExitStatus();
                if (0 != exitStatus) {
                    // container failed
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
                }
            }
            //stop infinite looping of run()
            //if (numCompletedContainers.get() + numFailedContainers.get() == numAllocatedContainers.get()
            //        && doneAllocating)
                //done = true;
        }

        public void onContainersAllocated(List<Container> allocatedContainers) {
            LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
            numAllocatedContainers.addAndGet(allocatedContainers.size());
            for (Container allocatedContainer : allocatedContainers) {
                LOG.info("Launching shell command on a new container." + ", containerId=" + allocatedContainer.getId()
                        + ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":"
                        + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
                        + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
                        + allocatedContainer.getResource().getMemory());

                LaunchAsterixContainer runnableLaunchContainer = new LaunchAsterixContainer(allocatedContainer,
                        containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer, "Asterix CC/NC");

                // I want to know if this node is the CC, because it must start before the NCs. 
                LOG.info("Allocated: " + allocatedContainer.getNodeId().getHost());
                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        /**
         * Ask the processes on the container to gracefully exit.
         */
        public void onShutdownRequest() {
            LOG.info("AM shutting down per request");
            //done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
            //TODO: This will become important when we deal with what happens if an NC dies
        }

        public float getProgress() {
            //return half way because progress is basically meaningless for us
            if (!doneAllocating) {
                return 0.0f;
            }
            return (float) 0.5;
        }

        public void onError(Throwable arg0) {
            LOG.error("Fatal Error recieved by AM: " + arg0);
            //done = true;
        }
    }

    private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }

        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }

        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
            }
        }

        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
        }

        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }

        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }

    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
     */
    private class LaunchCC implements Runnable {
        private String[] args;

        public LaunchCC(String[] args) {
            this.args = args;
        }

        public void run() {
            CCConfig ccConfig = new CCConfig();
            CmdLineParser cp = new CmdLineParser(ccConfig);
            try {
                cp.parseArgument(args);
            } catch (CmdLineException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            ClusterControllerService ccService;
            try {
                ccService = new ClusterControllerService(ccConfig);
                ccService.start();
                CCContext = ccService.getApplicationContext();
                CCContext.addClusterLifecycleListener(ccNotifications);
                while (!done) {
                    Thread.sleep(200);
                }
            } catch (InterruptedException e) {

            } catch (Exception e) {
                //not much to do?
                LOG.fatal("Could not start CC", e);
                //done = true;
                System.exit(-1);
            }
        }
    }

    private class LaunchAsterixContainer implements Runnable {

        // Allocated container
        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container
         * @param containerListener
         *            Callback handler of the container
         */
        public LaunchAsterixContainer(Container lcontainer, NMCallbackHandler containerListener) {
            this.container = lcontainer;
            this.containerListener = containerListener;
        }

        /**
         * Connects to CM, sets up container launch context
         * for shell command and eventually dispatches the container
         * start request to the CM.
         */
        public void run() {
            LOG.info("Setting up container launch container for containerid=" + container.getId());
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            // Set the local resources
            ctx.setLocalResources(localResources);

            //Set the env variables to be setup in the env where the application master will be run
            LOG.info("Set the environment for the node");
            Map<String, String> env = new HashMap<String, String>();

            // Add AppMaster.jar location to classpath
            // At some point we should not be required to add
            // the hadoop specific classpaths to the env.
            // It should be provided out of the box.
            // For now setting all required classpaths including
            // the classpath to "." for the application jar
            StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$()).append(File.pathSeparatorChar)
                    .append("./*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");

            // add the runtime classpath needed for tests to work
            if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
                classPathEnv.append(':');
                classPathEnv.append(System.getProperty("java.class.path"));
            }

            env.put("CLASSPATH", classPathEnv.toString());

            ctx.setEnvironment(env);
            LOG.info(ctx.getEnvironment().toString());
            /*
            try{
            writeAsterixConfig(clusterDesc);
            }
            catch (JAXBException | IOException e){
            	LOG.error("Couldn't write properites config file to disk.");
            	e.printStackTrace();
            	return;
            }
            */
            List<String> startCmd = null;
            if (obliterate) {
                startCmd = produceObliterateCommand(container);
            } else if (backup) {
                startCmd = produceBackupCommand(container);
            } else if (restore) {
                startCmd = produceRestoreCommand(container);
            } else {
                startCmd = produceStartCmd(container);
            }
            for (String s : startCmd) {
                LOG.info("Command to execute: " + s);
            }
            ctx.setCommands(startCmd);
            containerListener.addContainer(container.getId(), container);
            //finally start the container!?
            try {
                nodeIdContainerMap.put(containerToNode(container, clusterDesc).getId(), container.getId());
                //FIXME
            } catch (UnknownHostException e) {
            }
            nmClientAsync.startContainerAsync(container, ctx);
        }

        /**
         * Determines for a given container what the necessary command line
         * arguments are to start the Asterix processes on that instance
         * 
         * @param container
         *            The container to produce the commands for
         * @return A list of the commands that should be executed
         */
        private List<String> produceStartCmd(Container container) {
            List<String> commands = new ArrayList<String>();
            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);

            //first see if this node is the CC
            //now we need to know what node we are on, so we can apply the correct properties

            Node local;
            try {
                local = containerToNode(container, clusterDesc);
                LOG.info("Attempting to start NC on host " + local.getId());
                String iodevice = local.getIodevices();
                if (iodevice == null) {
                    iodevice = clusterDesc.getIodevices();
                }
                String opts = "JAVA_OPTS=" + NcJavaOpts + " ";
                vargs.add(opts + ASTERIX_NC_BIN_PATH + " -node-id " + local.getId());
                vargs.add("-cc-host " + InetAddress.getLocalHost().getHostAddress());
                vargs.add("-iodevices " + iodevice);
                vargs.add("-cluster-net-ip-address " + local.getClusterIp());
                vargs.add("-data-ip-address " + local.getClusterIp());
                vargs.add("-result-ip-address " + local.getClusterIp());
            } catch (UnknownHostException e) {
                LOG.error("Unable to find NC configured for host: " + container.getId() + e);
            }

            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        private List<String> produceObliterateCommand(Container container) {
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                LOG.error("Unable to find NC configured for host: " + container.getId() + e);
            }
            StringBuilder classPathEnv = new StringBuilder("").append("./*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(Environment.JAVA_HOME.$() + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(OBLITERATOR_CLASSNAME);
            for (String s : iodevices) {
                vargs.add(s + clusterDesc.getStore());
                LOG.debug("Deleting from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    vargs.add(clusterDesc.getTxnLogDir() + "txnLogs" + File.separator);
                    LOG.debug("Deleting logs from: " + clusterDesc.getTxnLogDir());
                    vargs.add(s + "asterix_root_metadata");
                }
            }
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        private List<String> produceBackupCommand(Container container) {
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                LOG.error("Unable to find NC configured for host: " + container.getId() + e);
                List<String> al = new ArrayList<String>();
                al.add("");
                return al;
            }
            StringBuilder classPathEnv = new StringBuilder("").append("./*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(Environment.JAVA_HOME.$() + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(HDFS_BACKUP_CLASSNAME);
            vargs.add("-backup");
            String dstBase = instanceConfPath + "backups" + "/" + backupTimestamp + "/" + local.getId();
            for (String s : iodevices) {
                List<String> ioComponents = Arrays.asList(s.split("\\/"));
                StringBuilder dst = new StringBuilder().append(dstBase);
                for (String io : ioComponents) {
                    dst.append(io);
                    if (ioComponents.indexOf(io) != ioComponents.size() - 1) {
                        dst.append("_");
                    }
                }
                dst.append("/");
                vargs.add(s + clusterDesc.getStore() + "," + dst);
                LOG.debug("Backing up from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    vargs.add(clusterDesc.getTxnLogDir() + "txnLogs" + File.separator + "," + dst);

                    LOG.debug("Backing up logs from: " + clusterDesc.getTxnLogDir());
                    vargs.add(s + "asterix_root_metadata" + "," + dst);
                }
            }
            LOG.debug("Backing up to: " + instanceConfPath + "backups" + "/" + local.getId());

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        private List<String> produceRestoreCommand(Container container) {
            Node local = null;
            List<String> iodevices = null;
            try {
                local = containerToNode(container, clusterDesc);
                if (local.getIodevices() == null) {
                    iodevices = Arrays.asList(clusterDesc.getIodevices().split(",", -1));
                } else {
                    iodevices = Arrays.asList(local.getIodevices().split(",", -1));
                }
            } catch (UnknownHostException e) {
                LOG.error("Unable to find NC configured for host: " + container.getId() + e);
            }
            StringBuilder classPathEnv = new StringBuilder("").append("./*");
            for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
                classPathEnv.append(File.pathSeparatorChar);
                classPathEnv.append(c.trim());
            }
            classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

            List<String> commands = new ArrayList<String>();
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
            vargs.add(Environment.JAVA_HOME.$() + File.separator + "bin" + File.separator + "java");
            vargs.add("-cp " + classPathEnv.toString());
            vargs.add(HDFS_BACKUP_CLASSNAME);
            vargs.add("-restore");
            String srcBase = instanceConfPath + "backups" + "/" + Long.parseLong(snapName) + "/" + local.getId();
            for (String s : iodevices) {
                List<String> ioComponents = Arrays.asList(s.split("\\/"));
                StringBuilder src = new StringBuilder().append(srcBase);
                for (String io : ioComponents) {
                    src.append(io);
                    if (ioComponents.indexOf(io) != ioComponents.size() - 1) {
                        src.append("_");
                    }
                }
                src.append("/");
                try {
                    FileSystem fs = FileSystem.get(conf);
                    FileStatus[] backups = fs.listStatus(new Path(src.toString()));
                    for (FileStatus b : backups) {
                        if (!b.getPath().toString().contains("txnLogs")
                                && !b.getPath().toString().contains("asterix_root_metadata")) {
                            vargs.add(b.getPath() + "," + s + clusterDesc.getStore());
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Could not stat backup directory in DFS");
                }
                vargs.add(src + "," + s + clusterDesc.getStore());
                LOG.debug("Restoring from: " + s);
                //logs only exist on 1st iodevice
                if (iodevices.indexOf(s) == 0) {
                    vargs.add(src + "txnLogs/" + "," + clusterDesc.getTxnLogDir() + File.separator);

                    LOG.debug("Restoring logs from: " + clusterDesc.getTxnLogDir());
                    vargs.add(src + "asterix_root_metadata" + "," + s);
                }
            }
            LOG.debug("Restoring to: " + instanceConfPath + "backups" + "/" + local.getId());

            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            commands.add(command.toString());
            return commands;
        }

        /**
         * Attempts to find the Node in the Cluster Description that matches this container
         * 
         * @param c
         *            The container to resolve
         * @return The node this container corresponds to
         * @throws java.net.UnknownHostException
         *             if the container isn't present in the description
         */
        private Node containerToNode(Container c, Cluster cl) throws UnknownHostException {
            String containerHost = c.getNodeId().getHost();
            InetAddress containerIp = InetAddress.getByName(containerHost);
            LOG.debug("Resolved Container IP: " + containerIp);
            for (Node node : cl.getNode()) {
                InetAddress nodeIp = InetAddress.getByName(node.getClusterIp());
                LOG.debug(nodeIp + "?=" + containerIp);
                if (nodeIp.equals(containerIp))
                    return node;
            }
            //if we find nothing, this is bad...
            throw new java.net.UnknownHostException("Could not resolve container" + containerHost + " to node");
        }

        /**
         * Determines whether or not a container is the one on which the CC should reside
         * 
         * @param c
         *            The container in question
         * @return True if the container should have the CC process on it, false otherwise.
         */

    }
}
