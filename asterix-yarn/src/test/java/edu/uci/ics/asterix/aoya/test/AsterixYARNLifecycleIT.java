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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.junit.AfterClass;
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
import edu.uci.ics.asterix.testframework.context.TestCaseContext;


public class AsterixYARNLifecycleIT {

    private static final int NUM_NC = 1;
    private static final String PATH_BASE = "src/test/resources/integrationts/lifecycle";
    private static final String PATH_ACTUAL = "ittest/";
    private static final Logger LOGGER = Logger.getLogger(AsterixYARNLifecycleIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;
    private static MiniYARNCluster miniCluster;
    private static Client aoyaClient;

    @BeforeClass
    public static void setUp() throws Exception {
        Configuration conf = new YarnConfiguration();
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniYARNCluster("Asterix - testing", NUM_NC, 1, 1);
        miniCluster.init(conf);
        miniCluster.start();

        //once the cluster is created, you can get its configuration
        //with the binding details to the cluster added from the minicluster
        YarnConfiguration appConf = new YarnConfiguration(miniCluster.getConfig());

        aoyaClient = new Client(appConf);
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        testCaseCollection = b.build(new File(PATH_BASE));
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixInstallerIntegrationUtil.deinit();
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
    public void testStopActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.ACTIVE);
            String command = "stop -n " + AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME;
            //cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == NUM_NC && !state.isCcRunning());
            LOGGER.info("Test stop active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testStartActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.INACTIVE);
            String command = "start -n " + AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME;
            //cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME);
            AsterixRuntimeState state = VerificationUtil.getAsterixRuntimeState(instance);
            assert (state.getFailedNCs().size() == 0 && state.isCcRunning());
            LOGGER.info("Test start active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void testDeleteActiveInstance() throws Exception {
        try {
            AsterixInstallerIntegrationUtil.transformIntoRequiredState(State.INACTIVE);
            String command = "delete -n " + AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME;
            //cmdHandler.processCommand(command.split(" "));
            AsterixInstance instance = ServiceProvider.INSTANCE.getLookupService().getAsterixInstance(
                    AsterixInstallerIntegrationUtil.ASTERIX_INSTANCE_NAME);
            assert (instance == null);
            LOGGER.info("Test delete active instance PASSED");
        } catch (Exception e) {
            throw new Exception("Test delete active instance " + "\" FAILED!", e);
        } finally {
            // recreate instance
            AsterixInstallerIntegrationUtil.createInstance();
        }
    }

    @Test
    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            TestsUtils.executeTest(PATH_ACTUAL, testCaseCtx, null, false);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixYARNLifecycleIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }

}
