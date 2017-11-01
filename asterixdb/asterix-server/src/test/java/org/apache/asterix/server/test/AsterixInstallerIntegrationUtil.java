/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.server.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.HDFSCluster;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hyracks.api.client.HyracksConnection;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.server.process.HyracksCCProcess;
import org.apache.hyracks.server.process.HyracksNCServiceProcess;
import org.apache.hyracks.server.process.HyracksVirtualCluster;

public class AsterixInstallerIntegrationUtil {

    // Important paths and files for this test.

    // The "target" subdirectory of asterix-server. All outputs go here.
    private static final String TARGET_DIR = StringUtils
            .join(new String[] { "target" }, File.separator);

    // Directory where the NCs create and store all data, as configured by
    // src/test/resources/NCServiceExecutionIT/cc.conf.
    private static final String INSTANCE_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "tmp" }, File.separator);

    // The log directory, where all CC, NCService, and NC logs are written. CC and
    // NCService logs are configured on the HyracksVirtualCluster below. NC logs
    // are configured in src/test/resources/NCServiceExecutionIT/ncservice*.conf.
    private static final String LOG_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "failsafe-reports" }, File.separator);

    // Directory where *.conf files are located.
    private static final String CONF_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "test-classes", "NCServiceExecutionIT" },
                    File.separator);

    // The app.home specified for HyracksVirtualCluster. The NCService expects
    // to find the NC startup script in ${app.home}/bin.
    private static final String APP_HOME = StringUtils
            .join(new String[] { TARGET_DIR, "appassembler" }, File.separator);

    // Path to the asterix-app directory. This is used as the current working
    // directory for the CC and NCService processes, which allows relative file
    // paths in "load" statements in test queries to find the right data. It is
    // also used for HDFSCluster.
    private static final String ASTERIX_APP_DIR = StringUtils
            .join(new String[] { "..", "asterix-app" },
                    File.separator);

    // Path to the actual AQL test files, which we borrow from asterix-app. This is
    // passed to TestExecutor.
    protected static final String TESTS_DIR = StringUtils
            .join(new String[] { ASTERIX_APP_DIR, "src", "test", "resources", "runtimets" },
                    File.separator);

    // Path that actual results are written to. We create and clean this directory
    // here, and also pass it to TestExecutor which writes the test output there.
    private static final String ACTUAL_RESULTS_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "ittest" }, File.separator);

    private static final Logger LOGGER = Logger.getLogger(AsterixInstallerIntegrationUtil.class.getName());

    enum KillCommand {
        CC,
        NC1,
        NC2;

        @Override
        public String toString() {
            return "<kill " + name().toLowerCase() + ">";
        }
    }

    private static HyracksCCProcess cc;
    private static HyracksNCServiceProcess nc1;
    private static HyracksNCServiceProcess nc2;
    private static HyracksVirtualCluster cluster;

    private static final TestExecutor testExecutor = new TestExecutor();


    public static void deinit() throws Exception {
    }

    public static void init(String clusterPath) throws Exception {
        File outDir = new File(ACTUAL_RESULTS_DIR);
        outDir.mkdirs();

        // Remove any instance data from previous runs.
        File instanceDir = new File(clusterPath);
        if (instanceDir.isDirectory()) {
            FileUtils.deleteDirectory(instanceDir);
        }

        cluster = new HyracksVirtualCluster(new File(APP_HOME), new File(ASTERIX_APP_DIR));
        nc1 = cluster.addNCService(
                new File(CONF_DIR, "ncservice1.conf"),
                new File(LOG_DIR, "ncservice1.log"));

        nc2 = cluster.addNCService(
                new File(CONF_DIR, "ncservice2.conf"),
                new File(LOG_DIR, "ncservice2.log"));

    }


    public static void createInstance() throws Exception {
        // Start CC
        cc = cluster.start(
                new File(CONF_DIR, "cc.conf"),
                new File(LOG_DIR, "cc.log"));

        testExecutor.waitForClusterActive(30, TimeUnit.SECONDS);
    }


    //public static void transformIntoRequiredState(AsterixInstance.State state) throws Exception {
    //}

    private static void deleteInstance() throws Exception {
        cluster.stop();
    }


    public static void installLibrary(String libraryName, String libraryDataverse, String libraryPath)
            throws Exception {
    }

    public static void uninstallLibrary(String dataverseName, String libraryName) throws Exception {
    }

    public static void executeCommand(String command) throws Exception {
   //     cmdHandler.processCommand(command.trim().split(" "));
    }

}
