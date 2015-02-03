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
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.filefilter.WildcardFileFilter;
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
public class AsterixYARNClient {

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
        RESTORE("restore"),
        NOOP("");

        public final String alias;

        Mode(String alias) {
            this.alias = alias;
        }

        public static Mode fromAlias(String a) {
            return STRING_TO_MODE.get(a.toLowerCase());
        }
    }

    public static final Map<String, AsterixYARNClient.Mode> STRING_TO_MODE = ImmutableMap
            .<String, AsterixYARNClient.Mode> builder().put(Mode.INSTALL.alias, Mode.INSTALL)
            .put(Mode.START.alias, Mode.START).put(Mode.STOP.alias, Mode.STOP).put(Mode.KILL.alias, Mode.KILL)
            .put(Mode.DESTROY.alias, Mode.DESTROY).put(Mode.ALTER.alias, Mode.ALTER)
            .put(Mode.LIBINSTALL.alias, Mode.LIBINSTALL).put(Mode.DESCRIBE.alias, Mode.DESCRIBE)
            .put(Mode.BACKUP.alias, Mode.BACKUP).put(Mode.LSBACKUP.alias, Mode.LSBACKUP)
            .put(Mode.RMBACKUP.alias, Mode.RMBACKUP).put(Mode.RESTORE.alias, Mode.RESTORE).build();
    private static final Log LOG = LogFactory.getLog(AsterixYARNClient.class);
    public static final String CONF_DIR_REL = ".asterix" + File.separator;
    private static final String instanceLock = "instance";
    private static String DEFAULT_PARAMETERS_PATH = "conf" + File.separator + "base-asterix-configuration.xml";
    private static String MERGED_PARAMETERS_PATH = "conf" + File.separator + "asterix-configuration.xml";
    private Mode mode = Mode.NOOP;

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
    private int amMemory = 1000;

    // Main class to invoke application master
    private final String appMasterMainClass = "edu.uci.ics.asterix.aoya.AsterixApplicationMaster";

    //instance name
    private String instanceName = "";
    //location of distributable asterix tarfile
    private String asterixZip = "";
    // Location of cluster configuration
    private String asterixConf = "";
    // Location of optional external libraries
    private String extLibs = "";

    private String instanceFolder = "";

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
    private String baseConfig = ".";

    /**
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {
        try {
            AsterixYARNClient client = new AsterixYARNClient();
            try {
                client.init(args);
                AsterixYARNClient.execute(client);
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

    public static void execute(AsterixYARNClient client) throws Exception {
        YarnClientApplication app;
        List<DFSResourceCoordinate> res;
        ApplicationId appId;
        switch (client.mode) {
            case START:
                app = client.makeApplicationContext();
                res = client.deployConfig();
                res.addAll(client.distributeBinaries());
                appId = client.deployAM(app, res, client.mode);
                LOG.info("Asterix started up with Application ID: " + appId.toString());
                if (Utils.waitForLiveness(appId, "Waiting for AsterixDB instance to resume ", client.yarnClient,
                        client.instanceName, client.conf)) {
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
                    AsterixYARNClient.killApplication(client.getLockFile(), client.yarnClient);
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
                    client.installAsterixConfig(false);
                    res = client.deployConfig();
                    res.addAll(client.distributeBinaries());

                    appId = client.deployAM(app, res, client.mode);
                    LOG.info("Asterix started up with Application ID: " + appId.toString());
                    if (Utils.waitForLiveness(appId, "Waiting for new AsterixDB Instance to start ", client.yarnClient,
                            client.instanceName, client.conf)) {
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
                client.installAsterixConfig(true);
                System.out.println("Configuration successfully modified");
                break;
            case DESTROY:
                try {
                    if (client.force
                            || Utils.confirmAction("Are you really sure you want to obliterate this instance? This action cannot be undone!")) {
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
                if (client.force || Utils.confirmAction("Performing a backup will stop a running instance.")) {
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
                if (client.force || Utils.confirmAction("Performing a restore will stop a running instance.")) {
                    app = client.makeApplicationContext();
                    res = client.deployConfig();
                    res.addAll(client.distributeBinaries());
                    client.restoreInstance(app, res);
                }
                break;
            default:
                LOG.fatal("Unknown mode. Known client modes are: start, stop, install, describe, kill, destroy, describe, backup, restore, lsbackup, rmbackup");
                client.printUsage();
                System.exit(-1);
        }
    }

    public AsterixYARNClient(Configuration conf) throws Exception {

        this.conf = conf;
        yarnClient = YarnClient.createYarnClient();
        //If the HDFS jars aren't on the classpath this won't be set 
        if (conf.get("fs.hdfs.impl", null) == conf.get("fs.file.impl", null)) { //only would happen if both are null
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        }
        yarnClient.init(conf);
        opts = parseConf(conf);
    }

    private static Options parseConf(Configuration conf) {
        Options opts = new Options();
        opts.addOption(new Option("appname", true, "Application Name. Default value - Asterix"));
        opts.addOption(new Option("priority", true, "Application Priority. Default 0"));
        opts.addOption(new Option("queue", true, "RM Queue in which this application is to be submitted"));
        opts.addOption(new Option("master_memory", true,
                "Amount of memory in MB to be requested to run the application master"));
        opts.addOption(new Option("log_properties", true, "log4j.properties file"));
        opts.addOption(new Option("n", "name", true, "Asterix instance name (required)"));
        opts.addOption(new Option("zip", "asterixZip", true,
                "zip file with AsterixDB inside- if in non-default location"));
        opts.addOption(new Option("bc", "baseConfig", true,
                "base Asterix parameters configuration file if not in default position"));
        opts.addOption(new Option("c", "asterixConf", true, "Asterix cluster config (required on install)"));
        opts.addOption(new Option("l", "externalLibs", true, "Libraries to deploy along with Asterix instance"));
        opts.addOption(new Option("ld", "libDataverse", true, "Dataverse to deploy external libraries to"));
        opts.addOption(new Option("r", "refresh", false,
                "If starting an existing instance, this will replace them with the local copy on startup"));
        opts.addOption(new Option("appId", true, "ApplicationID to monitor if running client in status monitor mode"));
        opts.addOption(new Option("masterLibsDir", true, "Directory that contains the JARs needed to run the AM"));
        opts.addOption(new Option("snapshot", true,
                "Backup timestamp for arguments requiring a specific backup (rm, restore)"));
        opts.addOption(new Option("v", "debug", false, "Dump out debug information"));
        opts.addOption(new Option("help", false, "Print usage"));
        opts.addOption(new Option("f", "force", false,
                "Execute this command as fully as possible, disregarding any caution"));
        return opts;
    }

    /**
   */
    public AsterixYARNClient() throws Exception {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Asterix YARN client. Usage: asterix [options] [mode]", opts);
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
        if (cliParser.hasOption("help")) {
            printUsage();
            return;
        }
        //initialize most things
        debugFlag = cliParser.hasOption("debug");
        force = cliParser.hasOption("force");
        baseConfig = cliParser.getOptionValue("baseConfig");
        extLibs = cliParser.getOptionValue("externalLibs");
        libDataverse = cliParser.getOptionValue("libDataverse");

        appName = cliParser.getOptionValue("appname", "AsterixDB");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "10"));

        instanceName = cliParser.getOptionValue("name");
        instanceFolder = instanceName + '/';
        appName = appName + ": " + instanceName;

        asterixConf = cliParser.getOptionValue("asterixConf");

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        //see if the given argument values are sane in general
        checkConfSanity(args, cliParser);

        //intialize the mode, see if it is a valid one.
        initMode(args, cliParser);

        //now check the validity of the arguments given the mode
        checkModeSanity(args, cliParser);

        //if we are going to refresh the binaries, find that out
        refresh = cliParser.hasOption("refresh");
        //same goes for snapshot restoration/removal
        snapName = cliParser.getOptionValue("snapshot");

        if (!cliParser.hasOption("asterixZip")
                && (mode == Mode.INSTALL || mode == Mode.ALTER || mode == Mode.DESTROY || mode == Mode.BACKUP)) {

            asterixZip = cliParser.getOptionValue("asterixZip", getAsterixDistributableLocation().getAbsolutePath());
        } else {
            asterixZip = cliParser.getOptionValue("asterixZip");
        }

    }

    private void checkConfSanity(String[] args, CommandLine cliParser) throws ParseException {
        String message = null;

        //Sanity check for no args 
        if (args.length == 0) {
            message = "No args specified for client to initialize";
        }
        //AM memory should be a sane value
        else if (amMemory < 0) {
            message = "Invalid memory specified for application master, exiting." + " Specified memory=" + amMemory;
        }
        //we're good!
        else {
            return;
        }
        //default:
        throw new ParseException(message);

    }

    private void initMode(String[] args, CommandLine cliParser) throws ParseException {
        List<String> clientVerb = cliParser.getArgList();
        String message = null;
        //Now check if there is a mode
        if (clientVerb == null || clientVerb.size() < 1) {
            message = "You must specify an action.";
        }
        //But there can only be one mode...
        else if (clientVerb.size() > 1) {
            message = "Trailing arguments, or too many arguments. Only one action may be performed at a time.";
        }
        if (message != null) {
            throw new ParseException(message);
        }
        //Now we can initialize the mode and check it against parameters
        Mode mode = Mode.fromAlias(clientVerb.get(0));
        if (mode == null) {
            mode = Mode.NOOP;
        }
    }

    private void checkModeSanity(String[] args, CommandLine cliParser) throws ParseException {

        String message = null;
        //The only time you can use the client without specifiying an instance, is to list all of the instances it sees.
        if (!cliParser.hasOption("name") && mode != Mode.DESCRIBE) {
            message = "You must give a name for the instance to be acted upon";
        } else if (mode == Mode.INSTALL && !cliParser.hasOption("asterixConf")) {
            message = "No Configuration XML given. Please specify a config for cluster installation";
        } else if (mode != Mode.START && cliParser.hasOption("refresh")) {
            message = "Cannot specify refresh in any mode besides start, mode is: " + mode;
        } else if (cliParser.hasOption("snapshot") && !(mode == Mode.RESTORE || mode == Mode.RMBACKUP)) {
            message = "Cannot specify a snapshot to restore in any mode besides restore or rmbackup, mode is: " + mode;
        } else if ((mode == Mode.ALTER || mode == Mode.INSTALL) && baseConfig == null
                && !(new File(DEFAULT_PARAMETERS_PATH).exists())) {
            message = "Default asterix parameters file is not in the default location, and no custom location is specified";
        }
        //nothing is wrong, so exit
        else {
            return;
        }
        //otherwise, something is bad.
        throw new ParseException(message);

    }

    private File getAsterixDistributableLocation() {
        //Look in the PWD for the "asterix" folder
        File tarDir = new File("asterix");
        if (!tarDir.exists()) {
            throw new IllegalArgumentException(
                    "Default directory structure not in use- please specify an asterix zip and base config file to distribute");
        }
        FileFilter tarFilter = new WildcardFileFilter("asterix-server*.zip");
        File[] tarFiles = tarDir.listFiles(tarFilter);
        if (tarFiles.length != 1) {
            throw new IllegalArgumentException(
                    "There is more than one canonically named asterix distributable in the default directory. Please leave only one there.");
        }
        return tarFiles[0];
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
            try {
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
            } catch (YarnException e) {
                LOG.warn("Stale lockfile detected, but the RM has no record of this application's last run. This is normal if the cluster was restarted.");
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
        asterixConfLoc.setVisibility(LocalResourceVisibility.PRIVATE);
        asterixConfLoc.setResource(ConverterUtils.getYarnUrlFromPath(dstConf));
        asterixConfLoc.setTimestamp(destStatus.getModificationTime());

        DFSResourceCoordinate conf = new DFSResourceCoordinate();
        conf.envs.put(dstConf.toUri().toString(), AConstants.CONFLOCATION);
        conf.envs.put(Long.toString(asterixConfLoc.getSize()), AConstants.CONFLEN);
        conf.envs.put(Long.toString(asterixConfLoc.getTimestamp()), AConstants.CONFTIMESTAMP);
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
        String commonsJarPattern = "^(commons).*(jar)$";
        String surefireJarPattern = "^(surefire).*(jar)$"; //for tests

        LOG.info(File.separator);
        for (String j : cp) {
            String[] pathComponents = j.split(Pattern.quote(File.separator));
            LOG.info(j);
            LOG.info(pathComponents[pathComponents.length - 1]);
            if (pathComponents[pathComponents.length - 1].matches(asterixJarPattern)
                    || pathComponents[pathComponents.length - 1].matches(commonsJarPattern)
                    || pathComponents[pathComponents.length - 1].matches(surefireJarPattern)) {
                LOG.info("Loading JAR: " + j);
                File f = new File(j);
                Path dst = new Path(fs.getHomeDirectory(), fullLibPath + f.getName());
                if (!fs.exists(dst) || refresh) {
                    fs.copyFromLocalFile(false, true, new Path(f.getAbsolutePath()), dst);
                }
                FileStatus dstSt = fs.getFileStatus(dst);
                LocalResource amLib = Records.newRecord(LocalResource.class);
                amLib.setType(LocalResourceType.FILE);
                amLib.setVisibility(LocalResourceVisibility.PRIVATE);
                amLib.setResource(ConverterUtils.getYarnUrlFromPath(dst));
                amLib.setTimestamp(dstSt.getModificationTime());
                amLib.setSize(dstSt.getLen());
                DFSResourceCoordinate amLibCoord = new DFSResourceCoordinate();
                amLibCoord.res = amLib;
                amLibCoord.name = f.getName();
                if (f.getName().contains("asterix-yarn") || f.getName().contains("surefire")) {
                    amLibCoord.envs.put(dst.toUri().toString(), AConstants.APPLICATIONMASTERJARLOCATION);
                    amLibCoord.envs.put(Long.toString(dstSt.getLen()), AConstants.APPLICATIONMASTERJARLEN);
                    amLibCoord.envs.put(Long.toString(dstSt.getModificationTime()),
                            AConstants.APPLICATIONMASTERJARTIMESTAMP);
                    amLibCoord.envs.put(conf.get("yarn.resourcemanager.address"), AConstants.RMADDRESS);
                    amLibCoord.envs.put(conf.get("yarn.resourcemanager.scheduler.address"),
                            AConstants.RMSCHEDULERADDRESS);
                }
                resources.add(amLibCoord);
            }
        }
        if (resources.size() == 0) {
            throw new IOException("Required JARs are missing. Please check your directory structure");
        }
        return resources;
    }

    private void installAsterixConfig(boolean overwrite) throws IllegalStateException, IOException {
        FileSystem fs = FileSystem.get(conf);
        File srcfile = new File(MERGED_PARAMETERS_PATH);
        Path src = new Path(srcfile.getCanonicalPath());
        String pathSuffix = CONF_DIR_REL + instanceFolder + File.separator + "asterix-configuration.xml";
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        if (fs.exists(dst) && !overwrite) {

            throw new IllegalStateException(
                    "Instance exists. Please delete an existing instance before trying to overwrite");
        }
        fs.copyFromLocalFile(false, true, src, dst);
    }

    public List<DFSResourceCoordinate> distributeBinaries() throws IOException, YarnException {

        List<DFSResourceCoordinate> resources = new ArrayList<DFSResourceCoordinate>(2);
        // Copy the application master jar to the filesystem
        // Create a local resource to point to the destination jar path
        FileSystem fs = FileSystem.get(conf);
        Path src, dst;
        FileStatus destStatus;
        String pathSuffix;

        // adding info so we can add the jar to the App master container path

        // Add the asterix tarfile to HDFS for easy distribution
        // Keep it all archived for now so add it as a file...

        pathSuffix = CONF_DIR_REL + instanceFolder + "asterix-server.zip";
        dst = new Path(fs.getHomeDirectory(), pathSuffix);
        if (refresh) {
            if (fs.exists(dst)) {
                fs.delete(dst, false);
            }
        }
        if (!fs.exists(dst)) {
            src = new Path(asterixZip);
            LOG.info("Copying Asterix distributable to DFS");
            fs.copyFromLocalFile(false, true, src, dst);
        }
        destStatus = fs.getFileStatus(dst);
        LocalResource asterixTarLoc = Records.newRecord(LocalResource.class);
        asterixTarLoc.setType(LocalResourceType.ARCHIVE);
        asterixTarLoc.setVisibility(LocalResourceVisibility.PRIVATE);
        asterixTarLoc.setResource(ConverterUtils.getYarnUrlFromPath(dst));
        asterixTarLoc.setTimestamp(destStatus.getModificationTime());

        // adding info so we can add the tarball to the App master container path
        DFSResourceCoordinate tar = new DFSResourceCoordinate();
        tar.envs.put(dst.toUri().toString(), AConstants.TARLOCATION);
        tar.envs.put(Long.toString(asterixTarLoc.getSize()), AConstants.TARLEN);
        tar.envs.put(Long.toString(asterixTarLoc.getTimestamp()), AConstants.TARTIMESTAMP);
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
            log4jRsrc.setVisibility(LocalResourceVisibility.PRIVATE);
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
        env.put(AConstants.INSTANCESTORE, CONF_DIR_REL + instanceFolder);
        env.put(AConstants.DFS_BASE, FileSystem.get(conf).getHomeDirectory().toUri().toString());

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
            classPathEnv.append(System.getProperty("path.separator"));
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
                    AsterixYARNClient.killApplication(appId, yarnClient);
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
                AsterixYARNClient.killApplication(appId, yarnClient);
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

        //this is the "base" config that is inside the zip, we start here
        AsterixConfiguration configuration;
        String configPathBase = MERGED_PARAMETERS_PATH;
        if (baseConfig != null) {
            configuration = Utils.loadAsterixConfig(baseConfig);
            configPathBase = new File(baseConfig).getParentFile().getAbsolutePath() + File.separator
                    + "asterix-configuration.xml";
            MERGED_PARAMETERS_PATH = configPathBase;
        } else {
            configuration = Utils.loadAsterixConfig(DEFAULT_PARAMETERS_PATH);
        }

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
            txnLogDir = node.getTxnLogDir() == null ? cluster.getTxnLogDir() : node.getTxnLogDir(); //node or cluster-wide
            txnLogDirs.add(new TransactionLogDir(node.getId(), txnLogDir
                    + (txnLogDir.charAt(txnLogDir.length() - 1) == File.separatorChar ? File.separator : "")
                    + "txnLogs" //if the string doesn't have a trailing / add one
                    + File.separator));
        }
        configuration.setMetadataNode(metadataNodeId);

        configuration.setCoredump(coredump);
        configuration.setTransactionLogDir(txnLogDirs);

        JAXBContext ctx = JAXBContext.newInstance(AsterixConfiguration.class);
        Marshaller marshaller = ctx.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
        FileOutputStream os = new FileOutputStream(MERGED_PARAMETERS_PATH);
        marshaller.marshal(configuration, os);
        os.close();
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
