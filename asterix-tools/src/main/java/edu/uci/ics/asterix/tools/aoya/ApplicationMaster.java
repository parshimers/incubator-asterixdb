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

package edu.uci.ics.asterix.tools.aoya;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.net.InetAddress;

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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

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
    private int appMasterRpcPort = 0;
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

    //HDFS path to Asterix distributable tarball
    private String asterixTarPath = "";
    // Timestamp needed for creating a local resource
    private long asterixTarTimestamp = 0;
    // File length needed for local resource
    private long asterixTarLen = 0;
    private int numTotalContainers = 0;

    private volatile boolean done;
    private volatile boolean success;

    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();

    private String CC = "";
    private String NC = "";
    private String IODevices = "";

    public static void main(String[] args) {

        boolean result = false;
        try {
            ApplicationMaster appMaster = new ApplicationMaster();
            LOG.info("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
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

        String cmd = "ls -al";
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

    public boolean init(String[] args) throws ParseException, IOException {

        Options opts = new Options();
        opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used unless for testing purposes");
        opts.addOption("asterixTar", true, "Asterix TAR file path");
        opts.addOption("CC", true, "CC host");
        opts.addOption("NC", true, "NC hosts");
        opts.addOption("IODevices", true, "IODevice Paths");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("debug", false, "Dump out debug information");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            printUsage(opts);
            throw new IllegalArgumentException("No args specified for application master to initialize");
        }

        if (cliParser.hasOption("help")) {
            printUsage(opts);
            return false;
        }

        if (cliParser.hasOption("debug")) {
            dumpOutDebugInfo();
        }

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

        LOG.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
                + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
                + appAttemptID.getAttemptId());

        CC = cliParser.getOptionValue("CC");
        NC = cliParser.getOptionValue("NC");
        IODevices = cliParser.getOptionValue("IODevices");
        asterixTarPath = cliParser.getOptionValue("asterixTar");

        asterixTarPath = envs.get(MConstants.LIBSLOCATION);
        asterixTarTimestamp = Long.parseLong(envs.get(MConstants.LIBSTIMESTAMP));
        asterixTarLen = Long.parseLong(envs.get(MConstants.LIBSLEN));

        return true;
    }

    private void printUsage(Options opts) {
        new HelpFormatter().printHelp("ApplicationMaster", opts);
    }

    @SuppressWarnings({ "unchecked" })
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

        // Setup local RPC Server to accept status requests directly from clients
        // TODO need to setup a protocol for client to be able to communicate to
        // the RPC server
        // TODO use the rpc port info to register with the RM for the client to
        // send requests to this app master

        // Register self with ResourceManager
        // This will start heartbeating to the RM
        try{
            appMasterHostname = InetAddress.getLocalHost().toString();
        }
        catch(java.net.UnknownHostException uhe){
            appMasterHostname = uhe.toString();
        }
        RegisterApplicationMasterResponse response = resourceManager.registerApplicationMaster(appMasterHostname,
                appMasterRpcPort, appMasterTrackingUrl);
        // Dump out information about cluster capability as seen by the
        // resource manager
        int maxMem = response.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);


        //request CC
        int numNodes = 0;
        ContainerRequest ccAsk = askForHost(CC);
        resourceManager.addContainerRequest(ccAsk);
        LOG.info("Asked for CC: " + CC);
        numNodes++;
        //now we wait to be given the CC before starting the NCs...
        while (ccUp.get() == false) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
            }
        }
        LOG.info("Waiting for CC to start...");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
        }
        //request NCs
        for (String s : NC.split(",")) {
            resourceManager.addContainerRequest(askForHost(s));
            LOG.info("Asked for NC: " + s);
            numNodes++;
        }
        System.out.println("Requested...");
        numRequestedContainers.set(numNodes);
        numTotalContainers = numNodes;

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }
        finish();
        return success;
    }

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
        //TODO Remove MOYA NODE

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
        done = true;
        resourceManager.stop();
    }

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
                    if (ContainerExitStatus.ABORTED != exitStatus) {
                        // shell script failed
                        // counts as completed
                        numCompletedContainers.incrementAndGet();
                        numFailedContainers.incrementAndGet();
                    } else {
                        // container was killed by framework, possibly preempted
                        // we should re-try as the container was lost for some reason
                        numAllocatedContainers.decrementAndGet();
                        numRequestedContainers.decrementAndGet();
                        // we do not need to release the container as it would be done
                        // by the RM
                    }
                } else {
                    // nothing to do
                    // container completed successfully
                    numCompletedContainers.incrementAndGet();
                    LOG.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId());
                }
            }
            //stop infinite looping of run()
            done = true;
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

                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer,
                        containerListener);
                Thread launchThread = new Thread(runnableLaunchContainer);

                if (allocatedContainer.getNodeId().getHost().equals(CC)) {
                    ccUp.set(true);
                }
                // launch and start the container on a separate thread to keep
                // the main thread unblocked
                // as all containers may not be allocated at one go.
                launchThreads.add(launchThread);
                launchThread.start();
            }
        }

        public void onShutdownRequest() {
            done = true;
        }

        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        public float getProgress() {
        //return half way because progress is basically meaningless for us
            return (float) 0.5;
        }

        public void onError(Throwable arg0) {
            done = true;
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
    private class LaunchContainerRunnable implements Runnable {

        // Allocated container
        Container container;

        NMCallbackHandler containerListener;

        /**
         * @param lcontainer
         *            Allocated container
         * @param containerListener
         *            Callback handler of the container
         */
        public LaunchContainerRunnable(Container lcontainer, NMCallbackHandler containerListener) {
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
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            LocalResource asterixTar = Records.newRecord(LocalResource.class);

            //this un-tar's the asterix distribution
            asterixTar.setType(LocalResourceType.ARCHIVE);

            // Set visibility of the resource
            // Setting to most private option
            asterixTar.setVisibility(LocalResourceVisibility.APPLICATION);
            Map<String, String> envs = System.getenv();
            // Set the resource to be copied over
            try {
                asterixTar.setResource(ConverterUtils.getYarnUrlFromURI(new URI(asterixTarPath)));

            } catch (URISyntaxException e) {
                LOG.error("Error locating Asterix tarball" + " in env, path=" + asterixTarPath);
                e.printStackTrace();
            }
            // Set timestamp and length of file so that the framework
            // can do basic sanity checks for the local resource
            // after it has been copied over to ensure it is the same
            // resource the client intended to use with the application
            LOG.debug("DSConstants Jar: " + MConstants.APPLICATIONMASTERJARLOCATION);
            LOG.debug("DSConstants Size: " + MConstants.APPLICATIONMASTERJARTIMESTAMP);
            LOG.debug("DSConstants TimeStamp: " + MConstants.APPLICATIONMASTERJARLEN);

            asterixTar.setTimestamp(asterixTarTimestamp);
            asterixTar.setSize(asterixTarLen);
            localResources.put("asterix-server", asterixTar);
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

            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);

            //now we need to know if we're on the NC or the CC
            String containerHost = container.getNodeId().getHost();
            InetAddress ccAddr;
            try {
                ccAddr = InetAddress.getByName(CC);
            } catch (java.net.UnknownHostException e) {
                ccAddr = null;
            }
            String ccIP = "nope";
            if (ccAddr != null)
                ccIP = ccAddr.getHostAddress();
            LOG.info(containerHost + ", " + CC);
            LOG.info("Resolved CC IP to " + ccIP);
            if (containerHost.equals(CC)) {
                vargs.add("cd asterix-server/bin;");
                vargs.add("export JAVA_OPTS=-Xmx1024m;");
                vargs.add("./asterixcc -cluster-net-ip-address 0.0.0.0 -client-net-ip-address 0.0.0.0");
            } else {
                vargs.add("cd asterix-server/bin;");
                vargs.add("export JAVA_OPTS=-Xmx1536m;");
                vargs.add("./asterixnc -node-id "+ containerHost.split("\\.")[0]);
                vargs.add("-cc-host "+ ccIP);
                vargs.add("-iodevices "+IODevices);
                vargs.add("-cluster-net-ip-address 0.0.0.0 ");
                vargs.add("-data-ip-address 0.0.0.0");
                vargs.add("-result-ip-address 0.0.0.0");
            }
            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(command.toString());
            ctx.setCommands(commands);

            containerListener.addContainer(container.getId(), container);
            //finally start the container!?
            nmClientAsync.startContainerAsync(container, ctx);
        }
    }

    private ContainerRequest askForHost(String host) {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(768);
        //we dont set anything because we don't care about that
        String[] hosts = new String[1];
        hosts[0] = host;
        // last flag must be false (relaxLocality)
        ContainerRequest request = new ContainerRequest(capability, hosts, null, pri, false);
        LOG.info("Requested host ask: " + request.getNodes());
        return request;
    }
}
