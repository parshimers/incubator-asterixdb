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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

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
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.collect.ImmutableMap;

import edu.uci.ics.asterix.common.configuration.AsterixConfiguration;
import edu.uci.ics.asterix.common.configuration.Coredump;
import edu.uci.ics.asterix.common.configuration.Store;
import edu.uci.ics.asterix.common.configuration.TransactionLogDir;
import edu.uci.ics.asterix.event.schema.yarnCluster.Cluster;
import edu.uci.ics.asterix.event.schema.yarnCluster.Node;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

    public static enum Mode {
        INSTALL("install"),
        START("start"),
        STOP("stop"),
        KILL("kill"),
        DESTROY("destroy"),
        ALTER("alter"),
        LIBINSTALL("libinstall"),
        DESCRIBE("describe"),
        BACKUP("backup"),
        LSBACKUP("lsbackups"),
        RMBACKUP("rmbackup"),
        RESTORE("restore");

        public final String alias;

        Mode(String alias) {
            this.alias = alias;
        }

        public static Mode fromAlias(String a) {
            return STRING_TO_MODE.get(a.toLowerCase());
        }
    }

    public static final Map<String, Client.Mode> STRING_TO_MODE = ImmutableMap.<String, Client.Mode> builder()
            .put(Mode.INSTALL.alias, Mode.INSTALL).put(Mode.START.alias, Mode.START).put(Mode.STOP.alias, Mode.STOP)
            .put(Mode.KILL.alias, Mode.KILL).put(Mode.DESTROY.alias, Mode.DESTROY).put(Mode.ALTER.alias, Mode.ALTER)
            .put(Mode.LIBINSTALL.alias, Mode.LIBINSTALL).put(Mode.DESCRIBE.alias, Mode.DESCRIBE)
            .put(Mode.BACKUP.alias, Mode.BACKUP).put(Mode.LSBACKUP.alias, Mode.LSBACKUP)
            .put(Mode.RMBACKUP.alias, Mode.RMBACKUP).put(Mode.RESTORE.alias, Mode.RESTORE).build();
    private static final Log LOG = LogFactory.getLog(Client.class);
    static final String CONF_DIR_REL = ".asterix" + File.separator;
    private static final String instanceLock = "instance";
    private static final String DEFAULT_PARAMETERS_PATH = "conf" + File.separator + "base-asterix-configuration.xml";
    private static final String MERGED_PARAMETERS_PATH = "conf" + File.separator + "asterix-configuration.xml";
    private Mode mode;

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
    private boolean refresh = false;
    private boolean force = false;

    // Command line options
    private Options opts;
    private String libDataverse;
    private String snapName = "";

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {
        try {
            Client client = new Client();
            try {
                client.init(args);
                switch (client.mode) {
                    case START:
                        YarnClientApplication app = client.makeApplicationContext();
                        List<DFSResourceCoordinate> res = client.deployConfig();
                        res.addAll(client.distributeBinaries());
                        ApplicationId appId = client.deployAM(app, res, client.mode);
                        LOG.info("Asterix started up with Application ID: " + appId.toString());
                        if (Utils.waitForLiveness(appId, "Waiting for AsterixDB instance to resume ",
                                client.yarnClient, client.instanceName, client.conf)) {
                            System.out.println("Asterix successfully deployed and is now running.");
                        } else {
                            LOG.fatal("AsterixDB appears to have failed to install and start");
                            System.exit(1);
                        }
                        break;
                    case STOP:
                        try {
                            client.stopInstance();
                        } catch (ApplicationNotFoundException e) {
                            System.out.println("Asterix instance by that name already exited or was never started");
                            client.deleteLockFile();
                        }
                        break;
                    case KILL:
                        if (client.isRunning()) {
                            Utils.confirmAction("Are you sure you want to kill this instance? In-progress tasks will be aborted");
                        }
                        try {
                            Client.killApplication(client.getLockFile(), client.yarnClient);
                        } catch (ApplicationNotFoundException e) {
                            System.out.println("Asterix instance by that name already exited or was never started");
                            client.deleteLockFile();
                        }
                        break;
                    case DESCRIBE:
                        Utils.listInstances(client.conf, CONF_DIR_REL);
                        break;
                    case INSTALL:
                        try {
                            app = client.makeApplicationContext();
                            client.installConfig();
                            client.writeAsterixConfig(Utils.parseYarnClusterConfig(client.asterixConf));
                            client.installAsterixConfig();
                            res = client.deployConfig();
                            res.addAll(client.distributeBinaries());

                            appId = client.deployAM(app, res, client.mode);
                            LOG.info("Asterix started up with Application ID: " + appId.toString());
                            if (Utils.waitForLiveness(appId, "Waiting for new AsterixDB Instance to start ",
                                    client.yarnClient, client.instanceName, client.conf)) {
                                System.out.println("Asterix successfully deployed and is now running.");
                            } else {
                                LOG.fatal("AsterixDB appears to have failed to install and start");
                                System.exit(1);
                            }
                        } catch (YarnException | IOException e) {
                            LOG.error("Asterix failed to deploy on to cluster");
                            throw e;
                        }
                        break;
                    case LIBINSTALL:
                        client.installExtLibs();
                        break;
                    case ALTER:
                        client.writeAsterixConfig(Utils.parseYarnClusterConfig(client.asterixConf));
                        client.installAsterixConfig();
                        System.out.println("Configuration successfully modified");
                        break;
                    case DESTROY:
                        try {
                            if (Utils
                                    .confirmAction("Are you really sure you want to obliterate this instance? This action cannot be undone!")) {
                                app = client.makeApplicationContext();
                                res = client.deployConfig();
                                res.addAll(client.distributeBinaries());
                                client.removeInstance(app, res);
                            }
                        } catch (YarnException | IOException e) {
                            LOG.error("Asterix failed to deploy on to cluster");
                            throw e;
                        }
                        break;
                    case BACKUP:
                        if (Utils.confirmAction("Performing a backup will stop a running instance.")) {
                            app = client.makeApplicationContext();
                            res = client.deployConfig();
                            res.addAll(client.distributeBinaries());
                            client.backupInstance(app, res);
                        }
                        break;
                    case LSBACKUP:
                        Utils.listBackups(client.conf, CONF_DIR_REL, client.instanceName);
                        break;
                    case RMBACKUP:
                        Utils.rmBackup(client.conf, CONF_DIR_REL, client.instanceName, Long.parseLong(client.snapName));
                        break;
                    case RESTORE:
                        if (Utils.confirmAction("Performing a restore will stop a running instance.")) {
                            app = client.makeApplicationContext();
                            res = client.deployConfig();
                            res.addAll(client.distributeBinaries());
                            client.restoreInstance(app, res);
                        }
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
        opts.addOption("refresh", false,
                "If starting an existing instance, this will replace them with the local copy on startup");
        opts.addOption("appId", true, "ApplicationID to monitor if running client in status monitor mode");
        opts.addOption("masterLibsDir", true, "Directory that contains the JARs needed to run the AM");
        opts.addOption("snapshot", true, "Backup timestamp for arguments requiring a specific backup (rm, restore)");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
        opts.addOption("force", false, "Execute this command as fully as possible, disregarding any caution");
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
        new HelpFormatter().printHelp("Asterix YARN client. Usage: asterix [mode] [options]", opts);
    }

    /**
     * Parse command line options
     * 
     * @param args
     *            Parsed command line options
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public void init(String[] args) throws ParseException {

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
        mode = Mode.fromAlias(clientVerb.get(0));

        if (cliParser.hasOption("help")) {
            printUsage();
            return;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }
        if (cliParser.hasOption("force")) {
            force = true;
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

        if (!cliParser.hasOption("name") && mode != Mode.DESCRIBE) {
            throw new IllegalArgumentException("You must give a name for the instance to be deployed/altered");
        }
        instanceName = cliParser.getOptionValue("name");
        instanceFolder = instanceName + '/';

        appName = appName + '-' + instanceName;

        if (!cliParser.hasOption("asterixTar") && (mode != Mode.INSTALL || cliParser.hasOption("refresh"))) {
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
        if (cliParser.hasOption("refresh") && mode == Mode.START) {
            refresh = true;
        } else if (cliParser.hasOption("refresh") && mode != Mode.START) {
            throw new IllegalArgumentException("Cannot specify refresh in any mode besides start, mode is: " + mode);
        }
        if (cliParser.hasOption("snapshot") && (mode == Mode.RESTORE || mode == Mode.RMBACKUP)) {
            snapName = cliParser.getOptionValue("snapshot");
        } else if (cliParser.hasOption("snapshot") && !(mode == Mode.RESTORE || mode == Mode.RMBACKUP)) {
            throw new IllegalArgumentException(
                    "Cannot specify a snapshot to restore in any mode besides restore, mode is: " + mode);
        }
    }

    /**
     * Main run function for the client
     * 
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public YarnClientApplication makeApplicationContext() throws IOException, YarnException {

        //first check to see if an instance already exists.
        FileSystem fs = FileSystem.get(conf);
        Path lock = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceFolder + instanceLock);
        LOG.info("Running Deployment");
        yarnClient.start();
        if (fs.exists(lock)) {
            ApplicationId lockAppId = getLockFile();
            ApplicationReport previousAppReport = yarnClient.getApplicationReport(lockAppId);
            YarnApplicationState prevStatus = previousAppReport.getYarnApplicationState();
            if (!(prevStatus == YarnApplicationState.FAILED || prevStatus == YarnApplicationState.KILLED || prevStatus == YarnApplicationState.FINISHED)
                    && mode != Mode.DESTROY && mode != Mode.BACKUP && mode != Mode.RESTORE) {
                throw new IllegalStateException("Instance is already running in: " + lockAppId);
            } else if (mode != Mode.DESTROY && mode != Mode.BACKUP && mode != Mode.RESTORE) {
                //stale lock file
                LOG.warn("Stale lockfile detected. Instance attempt " + lockAppId + " may have exited abnormally");
                deleteLockFile();
            }

        }

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
            throw new YarnException("Asterix instance by that name does not appear to exist in DFS");
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
        try {
            FileStatus st = fs.getFileStatus(dstConf);
            if (mode == Mode.INSTALL) {
                throw new IllegalStateException("Instance with this name already exists.");
            }
        } catch (FileNotFoundException e) {
            if (mode == Mode.START) {
                throw new IllegalStateException("Instance does not exist for this user");
            }
        }
        if (mode == Mode.INSTALL) {
            Path src = new Path(asterixConf);
            fs.copyFromLocalFile(false, true, src, dstConf);
        }

    }

    private void installExtLibs() throws IllegalStateException, IOException {
        FileSystem fs = FileSystem.get(conf);
        if (!instanceExists()) {
            throw new IllegalStateException("No instance by name " + instanceName + " found.");
        }
        if (isRunning()) {
            throw new IllegalStateException("Instance " + instanceName
                    + " is running. Please stop it before installing any libraries.");
        }
        String libPathSuffix = CONF_DIR_REL + instanceFolder + "library/" + libDataverse + '/';
        Path src = new Path(extLibs);
        String fullLibPath = libPathSuffix + src.getName();
        Path libFilePath = new Path(fs.getHomeDirectory(), fullLibPath);
        LOG.info("Copying Asterix external library to DFS");
        fs.copyFromLocalFile(false, true, src, libFilePath);
    }

    private List<DFSResourceCoordinate> installAmLibs() throws IllegalStateException, IOException {
        List<DFSResourceCoordinate> resources = new ArrayList<DFSResourceCoordinate>(2);
        FileSystem fs = FileSystem.get(conf);
        String fullLibPath = CONF_DIR_REL + instanceFolder + "am_jars/";
        String[] cp = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        String asterixJarPattern = "^(asterix).*(jar)$"; //starts with asterix,ends with jar
        LOG.info(File.separator);
        for (String j : cp) {
            String[] pathComponents = j.split(File.separator);
            LOG.info(j);
            LOG.info(pathComponents[pathComponents.length - 1]);
            if (pathComponents[pathComponents.length - 1].matches(asterixJarPattern)) {
                LOG.info("Loading JAR: " + j);
                File f = new File(j);
                Path dst = new Path(fs.getHomeDirectory(), fullLibPath + f.getName());
                if (!fs.exists(dst) || refresh) {
                    fs.copyFromLocalFile(false, true, new Path(f.getAbsolutePath()), dst);
                }
                FileStatus dstSt = fs.getFileStatus(dst);
                LocalResource amLib = Records.newRecord(LocalResource.class);
                amLib.setType(LocalResourceType.FILE);
                amLib.setVisibility(LocalResourceVisibility.APPLICATION);
                amLib.setResource(ConverterUtils.getYarnUrlFromPath(dst));
                amLib.setTimestamp(dstSt.getModificationTime());
                amLib.setSize(dstSt.getLen());
                DFSResourceCoordinate amLibCoord = new DFSResourceCoordinate();
                amLibCoord.res = amLib;
                amLibCoord.name = f.getName();
                resources.add(amLibCoord);
            }
        }
        return resources;
    }

    private void installAsterixConfig() throws IOException {
        if (!instanceExists()) {

        }
        FileSystem fs = FileSystem.get(conf);
        File srcfile = new File(MERGED_PARAMETERS_PATH);
        Path src = new Path(srcfile.getCanonicalPath());
        String pathSuffix = CONF_DIR_REL + instanceFolder + File.separator + "asterix-configuration.xml";
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        fs.copyFromLocalFile(false, true, src, dst);
    }

    public List<DFSResourceCoordinate> distributeBinaries() throws IOException, YarnException {

        List<DFSResourceCoordinate> resources = new ArrayList<DFSResourceCoordinate>(2);
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        Path src = new Path(appMasterJar);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "AppMaster.jar";
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        if (refresh) {
            if (fs.exists(dst)) {
                fs.delete(dst, false);
            }
        }
        if (!fs.exists(dst)) {
            fs.copyFromLocalFile(false, true, src, dst);
        }
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
        pathSuffix = CONF_DIR_REL + instanceFolder + "asterix-server.zip";
        dst = new Path(fs.getHomeDirectory(), pathSuffix);
        if (refresh) {
            if (fs.exists(dst)) {
                fs.delete(dst, false);
            }
        }
        if (!fs.exists(dst)) {
            LOG.info("Copying Asterix distributable to DFS");
            fs.copyFromLocalFile(false, true, src, dst);
        }
        destStatus = fs.getFileStatus(dst);
        LocalResource asterixTarLoc = Records.newRecord(LocalResource.class);
        asterixTarLoc.setType(LocalResourceType.ARCHIVE);
        asterixTarLoc.setVisibility(LocalResourceVisibility.PUBLIC);
        asterixTarLoc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        asterixTarLoc.setTimestamp(destStatus.getModificationTime());

        // adding info so we can add the tarball to the App master container path
        DFSResourceCoordinate tar = new DFSResourceCoordinate();
        tar.envs.put(dst.toUri().toString(), MConstants.TARLOCATION);
        tar.envs.put(Long.toString(asterixTarLoc.getSize()), MConstants.TARLEN);
        tar.envs.put(Long.toString(asterixTarLoc.getTimestamp()), MConstants.TARTIMESTAMP);
        tar.res = asterixTarLoc;
        tar.name = "asterix-server.zip";
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

        resources.addAll(installAmLibs());
        return resources;
    }

    public ApplicationId deployAM(YarnClientApplication app, List<DFSResourceCoordinate> resources, Mode mode)
            throws IOException, YarnException {

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
            if (res.envs == null) { //some entries may not have environment variables.
                continue;
            }
            for (Map.Entry<String, String> e : res.envs.entrySet()) {
                env.put(e.getValue(), e.getKey());
            }
        }
        ///add miscellaneous environment variables.
        env.put(MConstants.INSTANCESTORE, CONF_DIR_REL + instanceFolder);

        // Add AppMaster.jar location to classpath
        // At some point we should not be required to add
        // the hadoop specific classpaths to the env.
        // It should be provided out of the box.
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder("").append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("." + File.separator + "log4j.properties");

        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        LOG.debug("AM Classpath:" + classPathEnv.toString());
        env.put("CLASSPATH", classPathEnv.toString());

        amContainer.setEnvironment(env);

        // Set the necessary command to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);

        // Set java executable command
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$() + File.separator + "bin" + File.separator + "java");
        // Set class name
        vargs.add(appMasterMainClass);
        //Set params for Application Master
        vargs.add("-priority " + String.valueOf(moyaPriority));
        if (debugFlag) {
            vargs.add("-debug");
        }
        if (mode == Mode.DESTROY) {
            vargs.add("-obliterate");
        }
        if (mode == Mode.BACKUP) {
            vargs.add("-backup");
        }
        if (mode == Mode.RESTORE) {
            vargs.add("-restore " + snapName);
        }
        if (refresh) {
            vargs.add("-refresh");
        }
        //vargs.add("/bin/ls -alh asterix-server.zip/repo");
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "AppMaster.stderr");
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

        //now write the instance lock
        if (mode == Mode.INSTALL || mode == mode.START) {
            FileSystem fs = FileSystem.get(conf);
            Path lock = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceFolder + instanceLock);
            if (fs.exists(lock)) {
                throw new IllegalStateException("Somehow, this instance has been launched twice. ");
            }
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fs.create(lock, true)));
            try {
                out.write(app.getApplicationSubmissionContext().getApplicationId().toString());
                out.close();
            } finally {
                out.close();
            }
        }
        return app.getApplicationSubmissionContext().getApplicationId();

    }

    public static void killApplication(ApplicationId appId, YarnClient yarnClient) throws YarnException, IOException {
        if (appId == null) {
            throw new YarnException("No Application given to kill");
        }
        if (yarnClient.isInState(STATE.INITED)) {
            yarnClient.start();
        }
        YarnApplicationState st;
        ApplicationReport rep = yarnClient.getApplicationReport(appId);
        st = rep.getYarnApplicationState();
        if (st == YarnApplicationState.FINISHED || st == YarnApplicationState.KILLED
                || st == YarnApplicationState.FAILED) {
            throw new ApplicationNotFoundException("Application has already exited");
        }
        LOG.info("Killing applicaiton with ID: " + appId);
        yarnClient.killApplication(appId);

    }

    /**
     * Removes the configs. Eventually should run a YARN job to remove all tree artifacts.
     * 
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws YarnException
     * @throws JAXBException
     */
    private void removeInstance(YarnClientApplication app, List<DFSResourceCoordinate> resources)
            throws IllegalArgumentException, IOException, YarnException, JAXBException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        //if the instance is up, fix that
        if (isRunning()) {
            try {
                this.stopInstance();
            } catch (IOException | JAXBException e) {
                LOG.fatal("Could not stop nor kill instance gracefully.- instance lockfile may be stale");
            }
        } else if (!fs.exists(dstConf)) {
            throw new IllegalArgumentException("No instance configured with that name exists");
        }
        //now try deleting all of the on-disk artifacts on the cluster
        ApplicationId deleter = deployAM(app, resources, Mode.DESTROY);
        boolean delete_start = Utils.waitForApplication(deleter, yarnClient, "Waiting for deletion to start");
        if (!delete_start) {
            if (force) {
                fs.delete(new Path(CONF_DIR_REL + instanceFolder), true);
                LOG.error("Forcing deletion of HDFS resources");
            }
            LOG.fatal(" of on-disk persistient resources on individual nodes failed.");
            throw new YarnException();
        }
        boolean deleted = waitForCompletion(deleter, "Deletion in progress");
        if (!(deleted || force)) {
            LOG.fatal("Cleanup of on-disk persistent resources failed.");
            return;
        } else {
            fs.delete(new Path(CONF_DIR_REL + instanceFolder), true);
        }
        System.out.println("Deletion of instance succeeded.");

    }

    private void backupInstance(YarnClientApplication app, List<DFSResourceCoordinate> resources) throws IOException,
            YarnException, JAXBException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        //if the instance is up, fix that
        if (isRunning()) {
            try {
                this.stopInstance();
            } catch (IOException | JAXBException e) {
                LOG.fatal("Could not stop nor kill instance gracefully.- instance lockfile may be stale");
            }
        } else if (!fs.exists(dstConf)) {
            throw new IllegalArgumentException("No instance configured with that name exists");
        }
        ApplicationId backerUpper = deployAM(app, resources, Mode.BACKUP);
        boolean backupStart = Utils.waitForApplication(backerUpper, yarnClient,
                "Waiting for backup " + backerUpper.toString() + "to start");
        if (!backupStart) {
            LOG.fatal("Backup failed to start");
            throw new YarnException();
        }
        boolean complete = waitForCompletion(backerUpper, "Backup in progress");
        if (!complete) {
            LOG.fatal("Backup failed- timeout waiting for completion");
            return;
        }
        System.out.println("Backup of instance succeeded.");
    }

    private void restoreInstance(YarnClientApplication app, List<DFSResourceCoordinate> resources) throws IOException,
            YarnException, JAXBException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        //if the instance is up, fix that
        if (isRunning()) {
            try {
                this.stopInstance();
            } catch (IOException | JAXBException e) {
                LOG.fatal("Could not stop nor kill instance gracefully.- instance lockfile may be stale");
            }
        } else if (!fs.exists(dstConf)) {
            throw new IllegalArgumentException("No instance configured with that name exists");
        }
        //now try deleting all of the on-disk artifacts on the cluster
        ApplicationId restorer = deployAM(app, resources, Mode.RESTORE);
        boolean restoreStart = Utils.waitForApplication(restorer, yarnClient, "Waiting for restore to start");
        if (!restoreStart) {
            LOG.fatal("Restore failed to start");
            throw new YarnException();
        }
        boolean complete = waitForCompletion(restorer, "Restore in progress");
        if (!complete) {
            LOG.fatal("Restore failed- timeout waiting for completion");
            return;
        }
        System.out.println("Restoration of instance succeeded.");
    }

    /**
     * Stops the instance and remove the lockfile to allow a restart.
     * 
     * @throws IOException
     * @throws JAXBException
     * @throws YarnException
     */

    private void stopInstance() throws IOException, JAXBException, YarnException {
        ApplicationId appId = getLockFile();
        if (yarnClient.isInState(STATE.INITED)) {
            yarnClient.start();
        }
        System.out.println("Stopping instance " + instanceName);
        if (!isRunning()) {
            LOG.fatal("AsterixDB instance by that name is stopped already");
            return;
        }
        try {
            String ccIp = Utils.getCCHostname(instanceName, conf);
            Utils.sendShutdownCall(ccIp);
        } catch (IOException | JAXBException e) {
            if (force) {
                LOG.warn("Instance failed to stop gracefully, now killing it");
                try {
                    Client.killApplication(appId, yarnClient);
                } catch (YarnException e1) {
                    LOG.fatal("Could not stop nor kill instance gracefully.");
                    return;
                }
            }
        }
        //now make sure it is actually gone and not "stuck"
        String message = "Waiting for AsterixDB to shut down";
        boolean completed = waitForCompletion(appId, message);
        if (!completed && force) {
            LOG.warn("Instance failed to stop gracefully, now killing it");
            try {
                Client.killApplication(appId, yarnClient);
            } catch (YarnException e1) {
                LOG.fatal("Could not stop nor kill instance gracefully.");
                return;
            }
        }
        deleteLockFile();
    }

    private void deleteLockFile() throws IOException {
        if (instanceName == null || instanceName == "") {
            return;
        }
        FileSystem fs = FileSystem.get(conf);
        Path lockPath = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceName + '/' + instanceLock);
        if (fs.exists(lockPath)) {
            fs.delete(lockPath, false);
        }
    }

    private boolean instanceExists() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        return fs.exists(dstConf);
    }

    private boolean isRunning() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String pathSuffix = CONF_DIR_REL + instanceFolder + "cluster-config.xml";
        Path dstConf = new Path(fs.getHomeDirectory(), pathSuffix);
        if (fs.exists(dstConf)) {
            Path lock = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceFolder + instanceLock);
            return fs.exists(lock);
        } else {
            return false;
        }
    }

    private ApplicationId getLockFile() throws IOException, YarnException {
        if (instanceFolder == "") {
            throw new IllegalStateException("Instance name not given.");
        }
        FileSystem fs = FileSystem.get(conf);
        Path lockPath = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceFolder + instanceLock);
        if (!fs.exists(lockPath)) {
            throw new YarnException("Instance appears to not be running. If you know it is, try using kill");
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(lockPath)));
        String lockAppId = br.readLine();
        br.close();
        return ConverterUtils.toApplicationId(lockAppId);
    }

    public static ApplicationId getLockFile(String instanceName, Configuration conf) throws IOException {
        if (instanceName == "") {
            throw new IllegalStateException("Instance name not given.");
        }
        FileSystem fs = FileSystem.get(conf);
        Path lockPath = new Path(fs.getHomeDirectory(), CONF_DIR_REL + instanceName + '/' + instanceLock);
        if (!fs.exists(lockPath)) {
            return null;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(lockPath)));
        String lockAppId = br.readLine();
        br.close();
        return ConverterUtils.toApplicationId(lockAppId);
    }

    private void writeAsterixConfig(Cluster cluster) throws JAXBException, FileNotFoundException, IOException {
        String metadataNodeId = Utils.getMetadataNode(cluster).getId();
        String asterixInstanceName = instanceName;

        //this is the "base" config that is inside the tarball, we start here
        AsterixConfiguration configuration = loadAsterixConfig(DEFAULT_PARAMETERS_PATH);
        String version = Utils.getAsterixVersionFromClasspath();
        configuration.setVersion(version);

        configuration.setInstanceName(asterixInstanceName);
        String storeDir = null;
        List<Store> stores = new ArrayList<Store>();
        for (Node node : cluster.getNode()) {
            storeDir = node.getStore() == null ? cluster.getStore() : node.getStore();
            stores.add(new Store(node.getId(), storeDir));
        }
        configuration.setStore(stores);

        List<Coredump> coredump = new ArrayList<Coredump>();
        String coredumpDir = null;
        List<TransactionLogDir> txnLogDirs = new ArrayList<TransactionLogDir>();
        String txnLogDir = null;
        for (Node node : cluster.getNode()) {
            coredumpDir = node.getLogDir() == null ? cluster.getLogDir() : node.getLogDir();
            coredump.add(new Coredump(node.getId(), coredumpDir + "coredump" + File.separator));
            txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir();
            txnLogDirs.add(new TransactionLogDir(node.getId(), txnLogDir + "txnLogs" + File.separator));
        }
        configuration.setMetadataNode(metadataNodeId);

        configuration.setCoredump(coredump);
        configuration.setTransactionLogDir(txnLogDirs);

        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        String cwd = new File(".").getAbsolutePath() + "/";
        FileOutputStream os = new FileOutputStream(cwd + MERGED_PARAMETERS_PATH);
        marshaller.marshal(configuration, os);
        os.close();
    }

    private AsterixConfiguration loadAsterixConfig(String path) throws IOException, JAXBException {
        File f = new File(path);
        JAXBContext configCtx = JAXBContext.newInstance(AsterixConfiguration.class);
        Unmarshaller unmarshaller = configCtx.createUnmarshaller();
        AsterixConfiguration conf = (AsterixConfiguration) unmarshaller.unmarshal(f);
        return conf;
    }

    private boolean waitForCompletion(ApplicationId appId, String message) throws YarnException, IOException,
            JAXBException {
        return Utils.waitForApplication(appId, yarnClient, message);
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
