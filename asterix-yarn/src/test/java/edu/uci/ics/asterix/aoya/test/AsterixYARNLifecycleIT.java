/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aoya.test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.aoya.Client;
import edu.uci.ics.asterix.event.error.VerificationUtil;
import edu.uci.ics.asterix.event.model.AsterixInstance;
import edu.uci.ics.asterix.event.model.AsterixInstance.State;
import edu.uci.ics.asterix.event.model.AsterixRuntimeState;
import edu.uci.ics.asterix.event.service.ServiceProvider;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.aoya.test.YARNCluster;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

public class AsterixYARNLifecycleIT {

    private static final int NUM_NC = 1;
    private static final String PATH_BASE = "src/test/resources/integrationts/lifecycle";
    private static final String PATH_ACTUAL = "ittest/";
    private static final Logger LOGGER = Logger.getLogger(AsterixYARNLifecycleIT.class.getName());
    private static final String INSTANCE_NAME = "asterix-integration-test";
    //private static List<TestCaseContext> testCaseCollection;
    private static MiniYARNCluster miniCluster;
    private static YarnConfiguration appConf;
    private static String aoyaHome;
    private static String configPath;
    private static String aoyaServerPath;
    private static String parameterPath;
    private static Client aoyaClient;

    @BeforeClass
    public static void setUp() throws Exception {
        File asterixProjectDir = new File(System.getProperty("user.dir"));

        File installerTargetDir = new File(asterixProjectDir, "target");

        String[] dirsInTarget = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-yarn")
                        && name.endsWith("binary-assembly");
            }

        });
        if (dirsInTarget.length != 1) {
            throw new IllegalStateException("Could not find binary to run YARN integration test with");
        }
        aoyaHome = installerTargetDir.getAbsolutePath() + File.separator + dirsInTarget[0];
        File asterixServerInstallerDir = new File(aoyaHome, "asterix");
        String[] zipsInFolder = asterixServerInstallerDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("asterix-server") && name.endsWith("binary-assembly.zip");
            }
        });
        if (zipsInFolder.length != 1) {
            throw new IllegalStateException("Could not find server binary to run YARN integration test with");
        }
        aoyaServerPath = asterixServerInstallerDir.getAbsolutePath() + File.separator + zipsInFolder[0];
        configPath = aoyaHome + File.separator + "configs" + File.separator + "local.xml";
        parameterPath = aoyaHome + File.separator + "conf" + File.separator + "base-asterix-configuration.xml";
        YARNCluster.getInstance().setup();
        miniCluster = YARNCluster.getInstance().getCluster();
        miniCluster.start();

        //once the cluster is created, you can get its configuration
        //with the binding details to the cluster added from the minicluster
        appConf = new YarnConfiguration(miniCluster.getConfig());
        FileSystem fs = FileSystem.get(appConf);
        Path instanceState = new Path(Client.CONF_DIR_REL + INSTANCE_NAME + "/");
        System.out.println(instanceState);
        fs.delete(instanceState, true);
        Assert.assertFalse(fs.exists(instanceState));

        //TestCaseContext.Builder b = new TestCaseContext.Builder();
        //testCaseCollection = b.build(new File(PATH_BASE));
        aoyaClient = new Client(appConf);
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        //List<TestCaseContext> testCaseCollection = b.build(new File(PATH_BASE));
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        miniCluster.close();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        return testArgs;
    }

    @Test
    public void testInstallActiveInstance() throws Exception {
        String command ="-n " + INSTANCE_NAME + " -c " + configPath + " -bc " + parameterPath +" -tar " + aoyaServerPath + " install";

        Client aoyaClient = new Client(appConf);
        aoyaClient.init(command.split(" "));
        Client.execute(aoyaClient);
    }

    @Test
    public void testStopActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " stop";
        Client aoyaClient = new Client(appConf);
        aoyaClient.init(command.split(" "));
        Client.execute(aoyaClient);
    }

    @Test
    public void testStartActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " start";
        Client aoyaClient = new Client(appConf);
        aoyaClient.init(command.split(" "));
        Client.execute(aoyaClient);
    }

    @Test
    public void testDeleteActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " delete";
        Client aoyaClient = new Client(appConf);
        aoyaClient.init(command.split(" "));
        Client.execute(aoyaClient);
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixYARNLifecycleIT();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }

}
