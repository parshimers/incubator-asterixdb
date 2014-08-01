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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    private static final Log LOG = LogFactory.getLog(Client.class);
    private static final String CONF_DIR_REL = ".asterix/";
    private static String mode;

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;
    // Application master specific info to register a new Application with
    // RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 64;

    // Application master jar file
    private String appMasterJar = "";
    // Main class to invoke application master
    private final String appMasterMainClass = "edu.uci.ics.asterix.aoya.ApplicationMaster";

    //instance name
    private String instanceName = "";
    //location of distributable asterix tarfile
    private String asterixTar = "";
    // Location of cluster configuration
    private String asterixConf = "";
    // Location of optional external libraries
    private String extLibs = "";

    private String instanceFolder = "";

    // Shell Command Container priority
    private int moyaPriority = 0;

    // Amt of memory to request for container in which shell script will be
    // executed
    private int containerMemory = 256;
    // No. of containers in which the shell script needs to be executed
    private int numContainers = 1;

    // log4j.properties file
    // if available, add to local resources and set into classpath
    private String log4jPropFile = "";

    // Debug flag
    boolean debugFlag = false;

    ApplicationId appId;

    // Command line options
    private Options opts;
    private String libDataverse;

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {
        try {
            Client client = new Client();
            try {
                String verb = client.init(args);
                switch (verb) {
                    case "start":
                        YarnClientApplication app = client.makeApplicationContext();
                        List<DFSResourceCoordinate> res = client.deployConfig();
                        res.addAll(client.distributeBinaries());
                        LOG.info("Asterix deployed and running with ID: " + client.deployAM(app, res).toString());
                        break;
                    case "kill":
                        client.killApplication();
                        break;
                    case "describe":
                        break;
                    case "install":
                        try {
                            app = client.makeApplicationContext();
                            client.installConfig();
                            res = client.deployConfig();
                            res.addAll(client.distributeBinaries());
                            LOG.info("Asterix deployed and running with ID: " + client.deployAM(app, res).toString());
                        } catch (YarnException | IOException e) {
                            LOG.error("Asterix failed to deploy on to cluster");
                            throw e;
                        }
                        break;
                    case "libinstall":
                        client.installLibs();
                        break;
                    case "alter":
                        break;
                    case "status":
                        client.monitorApplication();
                        break;
                    case "destroy":
                        client.removeInstance();
                        break;
                    default:
                        LOG.fatal("Unknown action. Known actions are: start, stop, install, status, kill");
                        client.printUsage();
                        System.exit(-1);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                e.printStackTrace();
                client.printUsage();
                System.exit(-1);
            }
        } catch (Throwable t) {
            LOG.fatal("Error running client", t);
            System.exit(1);
        }
        LOG.info("Command executed successfully.");
        System.exit(0);
    }

    public Client(Configuration conf) throws Exception {

        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        //If the HDFS jars aren't on the classpath this won't be set 
        if (conf.get("fs.hdfs.impl", null) == conf.get("fs.file.impl", null)) { //only would happen if both are null
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        }
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - Asterix");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("jar", true, "Jar file containing the application master");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("name", true, "Asterix instance name (required)");
        opts.addOption("asterixTar", true, "tarball with Asterix inside");
        opts.addOption("asterixConf", true, "Asterix cluster config");
        opts.addOption("externalLibs", true, "Libraries to deploy along with Asterix instance");
        opts.addOption("libDataverse", true, "Dataverse to deploy external libraries to");
        opts.addOption("appId", true, "ApplicationID to monitor if running client in status monitor mode");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    /**
   */
    public Client() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Asterix YARN client. Usage: ./asterix [mode] [options]", opts);
    }

    /**
     * Parse command line options
     * 
     * @param args
     *            Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public String init(String[] args) throws ParseException {

        CommandLine cliParser = new GnuParser().parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }

        List<String> clientVerb = cliParser.getArgList();
        if (clientVerb == null || clientVerb.size() < 1) {
            LOG.fatal("Too few arguments.");
            throw new IllegalArgumentException();
        }
        if (clientVerb.size() > 1) {
            LOG.fatal("Too many arguments.");
            throw new IllegalArgumentException();
        }
        mode = clientVerb.get(0);

        if (cliParser.hasOption("help")) {
            printUsage();
            return new String();
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        if (cliParser.hasOption("appId")) {
            LOG.info(cliParser.getOptionValue("appId"));
            appId = ConverterUtils.toApplicationId(cliParser.getOptionValue("appId"));
            //we do not care about any of the other stuff that is more for the installation
            return mode;
        }

        appName = cliParser.getOptionValue("appname", "Asterix");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }

        if (!cliParser.hasOption("jar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }

        appMasterJar = cliParser.getOptionValue("jar");

        if (!cliParser.hasOption("name")) {
            throw new IllegalArgumentException("You must give a name for the instance to be deployed/altered");
        }
        instanceName = cliParser.getOptionValue("name");
        instanceFolder = instanceName + '/';

        appName = appName + '-' + instanceName;

        if (!cliParser.hasOption("asterixTar")) {
            throw new IllegalArgumentException("You must include an Asterix TAR to distribute!");
        }
        asterixTar = cliParser.getOptionValue("asterixTar");
        if (!cliParser.hasOption("asterixTar")) {
            throw new IllegalArgumentException("You must specify a cluster configuration");
        }
        asterixConf = cliParser.getOptionValue("asterixConf");
        if (cliParser.hasOption("externalLibs")) {
            extLibs = cliParser.getOptionValue("externalLibs");
            if (cliParser.hasOption("libDataverse")) {
                libDataverse = cliParser.getOptionValue("libDataverse");
            }
        }
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "128"));
        if (containerMemory < 0 || numContainers < 1) {
            throw new IllegalArgumentException("Invalid no. of containers or container memory specified, exiting."
                    + " Specified containerMemory=" + containerMemory + ", numContainer=" + numContainers);
        }
        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        return mode;
    }

    /**
     * Main run function for the client
     * 
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public YarnClientApplication makeApplicationContext() throws IOException, YarnException {

        LOG.info("Running Deployment");
        yarnClient.start();

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

        // A resource ask cannot exceed the max.
        if (amMemory > maxMem) {
            LOG.info("AM memory specified above max threshold of cluster. Using max value." + ", specified=" + amMemory
                    + ", max=" + maxMem);
            amMemory = maxMem;
        }

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();
        appContext.setApplicationName(appName);

        return app;
    }

    private List<DFSResourceCoordinate> deployConfig() throws YarnException, IOException {

        FileSystem fs = FileSystem.get(conf);
        List<DFSResourceCoordinate> resources = new ArrayList<DFSResourceCoordinate>(2);

        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        FileStatus destStatus;
        try {
            destStatus = fs.getFileStatus(dstConf);
        } catch (IOException e) {
            throw new YarnException("Error placing or locating config file in DFS- exiting");
        }
        LocalResource asterixConfLoc = Records.newRecord(LocalResource.class);
        asterixConfLoc.setType(LocalResourceType.FILE);
        asterixConfLoc.setVisibility(LocalResourceVisibility.PUBLIC);
        asterixConfLoc.setResource(ConverterUtils.getYarnUrlFromPath(dstConf));
        asterixConfLoc.setTimestamp(destStatus.getModificationTime());

        DFSResourceCoordinate conf = new DFSResourceCoordinate();
        conf.envs.put(dstConf.toUri().toString(), MConstants.CONFLOCATION);
        conf.envs.put(Long.toString(asterixConfLoc.getSize()), MConstants.CONFLEN);
        conf.envs.put(Long.toString(asterixConfLoc.getTimestamp()), MConstants.CONFTIMESTAMP);
        conf.name = "cluster-config.xml";
        conf.res = asterixConfLoc;
        resources.add(conf);

        return resources;

    }

    private void installConfig() throws YarnException, IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        boolean existing = false;
        try {
            FileStatus st = fs.getFileStatus(dstConf);
            if (mode.equals("install")) {
                throw new IllegalStateException("Instance with this name already exists.");
            }
        } catch (FileNotFoundException e) {
            if (mode.equals("start")) {
                throw new IllegalStateException("Instance does not exist for this user");
            }
        }
        if (mode.equals("install")) {
            Path src = new Path(asterixConf);
            fs.copyFromLocalFile(false, true, src, dstConf);
            existing = true;
        }

    }

    private void installLibs() throws IllegalStateException, IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        if (!fs.exists(dstConf)) {
            throw new IllegalStateException("Could not find existing asterix instance in DFS");
        }
        String libPathSuffix = CONF_DIR_REL + instanceFolder + "library/" + libDataverse + '/';
        Path libDst = new Path(fs.getHomeDirectory(), libPathSuffix);
        Path src = new Path(extLibs);
        String fullLibPath = libPathSuffix + src.getName();
        Path libFilePath = new Path(fs.getHomeDirectory(), fullLibPath);
        LOG.info("Copying Asterix external library to DFS");
        fs.copyFromLocalFile(false, true, src, libFilePath);
    }

    public List<DFSResourceCoordinate> distributeBinaries() throws IOException, YarnException {

        List<DFSResourceCoordinate> resources = new ArrayList<DFSResourceCoordinate>(2);
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(appMasterJar);
        String pathSuffix = appName + "/" + appId.getId() + "/AppMaster.jar";
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class);

        // Set the type of resource - file or archive
        // archives are untarred at destination
        // we don't need the jar file to be untarred
        amJarRsrc.setType(LocalResourceType.FILE);
        // Set visibility of the resource
        // Setting to most private option
        amJarRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        // Set the resource to be copied over
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        // Set timestamp and length of file so that the framework
        // can do basic sanity checks for the local resource
        // after it has been copied over to ensure it is the same
        // resource the client intended to use with the application
        amJarRsrc.setTimestamp(destStatus.getModificationTime());
        amJarRsrc.setSize(destStatus.getLen());

        // adding info so we can add the jar to the App master container path
        DFSResourceCoordinate am = new DFSResourceCoordinate();
        am.envs.put(dst.toUri().toString(), MConstants.APPLICATIONMASTERJARLOCATION);
        FileStatus amFileStatus = fs.getFileStatus(dst);
        am.envs.put(Long.toString(amJarRsrc.getSize()), MConstants.APPLICATIONMASTERJARLEN);
        am.envs.put(Long.toString(amJarRsrc.getTimestamp()), MConstants.APPLICATIONMASTERJARTIMESTAMP);
        am.res = amJarRsrc;
        am.name = "AppMaster.jar";
        resources.add(am);

        // Add the asterix tarfile to HDFS for easy distribution
        // Keep it all archived for now so add it as a file...
        src = new Path(asterixTar);
        pathSuffix = appName + "/" + appId.getId() + "/asterix-server.tar";
        dst = new Path(fs.getHomeDirectory(), pathSuffix);
        LOG.info("Copying Asterix distributable to DFS");
        fs.copyFromLocalFile(false, true, src, dst);
        destStatus = fs.getFileStatus(dst);
        LocalResource asterixTarLoc = Records.newRecord(LocalResource.class);
        asterixTarLoc.setType(LocalResourceType.FILE);
        asterixTarLoc.setVisibility(LocalResourceVisibility.APPLICATION);
        asterixTarLoc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        asterixTarLoc.setTimestamp(destStatus.getModificationTime());

        // adding info so we can add the tarball to the App master container path
        DFSResourceCoordinate tar = new DFSResourceCoordinate();
        tar.envs.put(dst.toUri().toString(), MConstants.TARLOCATION);
        tar.envs.put(Long.toString(asterixTarLoc.getSize()), MConstants.TARLEN);
        tar.envs.put(Long.toString(asterixTarLoc.getTimestamp()), MConstants.TARTIMESTAMP);
        tar.res = asterixTarLoc;
        tar.name = "asterix-server.tar";
        resources.add(tar);

        // Set the log4j properties if needed
        if (!log4jPropFile.isEmpty()) {
            Path log4jSrc = new Path(log4jPropFile);
            Path log4jDst = new Path(fs.getHomeDirectory(), "log4j.props");
            fs.copyFromLocalFile(false, true, log4jSrc, log4jDst);
            FileStatus log4jFileStatus = fs.getFileStatus(log4jDst);
            LocalResource log4jRsrc = Records.newRecord(LocalResource.class);
            log4jRsrc.setType(LocalResourceType.FILE);
            log4jRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
            log4jRsrc.setResource(ConverterUtils.getYarnUrlFromURI(log4jDst.toUri()));
            log4jRsrc.setTimestamp(log4jFileStatus.getModificationTime());
            log4jRsrc.setSize(log4jFileStatus.getLen());
            DFSResourceCoordinate l4j = new DFSResourceCoordinate();
            tar.res = log4jRsrc;
            tar.name = "log4j.properties";
            resources.add(l4j);
        }

        return resources;
    }

    public ApplicationId deployAM(YarnClientApplication app, List<DFSResourceCoordinate> resources) throws IOException,
            YarnException {

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        // Set local resource info into app master container launch context
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        for (DFSResourceCoordinate res : resources) {
            localResources.put(res.name, res.res);
        }
        amContainer.setLocalResources(localResources);
        // Set the env variables to be setup in the env where the application
        // master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();

        // using the env info, the application master will create the correct
        // local resource for the
        // eventual containers that will be launched to execute the shell
        // scripts
        for (DFSResourceCoordinate res : resources) {
            if (res.envs == null) //some entries may not have environment variables.
                continue;
            for (Map.Entry<String, String> e : res.envs.entrySet()) {
                env.put(e.getValue(), e.getKey());
            }
        }
        ///add miscellaneous environment variables.
        env.put(MConstants.PATHSUFFIX, appName + "/" + appId.getId());
        env.put(MConstants.INSTANCESTORE, CONF_DIR_REL + instanceFolder);

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

        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        // Set class name
        vargs.add(appMasterMainClass);
        // Set params for Application Master
        vargs.add("-priority " + String.valueOf(moyaPriority));
        if (debugFlag) {
            vargs.add("-debug");
        }
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Completed setting up app master command " + command.toString());
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());
        amContainer.setCommands(commands);

        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);

        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);

        // The following are not required for launching an application master
        // amContainer.setContainerId(containerId);

        appContext.setAMContainerSpec(amContainer);

        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide?
        pri.setPriority(amPriority);
        appContext.setPriority(pri);

        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);

        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp =
        // applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on
        // success
        // or an exception thrown to denote some form of a failure
        LOG.info("Submitting application to ASM");

        yarnClient.submitApplication(appContext);

        // TODO
        // Try submitting the same request again
        // app submission failure?

        // Monitor the application
        return appId;

    }

    /**
     * Monitor the submitted application for completion. Kill application if
     * time expires.
     * 
     * @param appId
     *            Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication() throws YarnException, IOException {

        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }

            // Get application report for the appId we are interested in
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for" + ", appId=" + appId.getId() + ", clientToAMToken="
                    + report.getClientToAMToken() + ", appDiagnostics=" + report.getDiagnostics() + ", appMasterHost="
                    + report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
                    + report.getRpcPort() + ", appStartTime=" + report.getStartTime() + ", yarnAppState="
                    + report.getYarnApplicationState().toString() + ", distributedFinalState="
                    + report.getFinalApplicationStatus().toString() + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;
                } else {
                    LOG.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
                            + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
                    return false;
                }
            } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish." + " YarnState=" + state.toString() + ", DSFinalStatus="
                        + dsStatus.toString() + ". Breaking monitoring loop");
                return false;
            }
        }

    }

    private void killApplication() throws YarnException, IOException {
        if (appId == null) {
            throw new YarnException("No Application given to kill");
        }
        ApplicationReport rep = yarnClient.getApplicationReport(appId);
        YarnApplicationState st = rep.getYarnApplicationState();
        if (st == YarnApplicationState.FINISHED || st == YarnApplicationState.KILLED
                || st == YarnApplicationState.FAILED) {
            throw new YarnException("Application has already exited");
        }
        LOG.info("Killing applicaiton with ID: " + appId);
        yarnClient.killApplication(appId);

    }

    /**
     * Removes the configs. Eventually should run a YARN job to remove all tree artifacts.
     * 
     * @throws IOException
     * @throws IllegalArgumentException
     */
    private void removeInstance() throws IllegalArgumentException, IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(CONF_DIR_REL + instanceFolder), true);
    }

    private class DFSResourceCoordinate {
        String name;
        LocalResource res;
        Map<String, String> envs;

        public DFSResourceCoordinate() {
            envs = new HashMap<String, String>(3);
        }
    }
}
