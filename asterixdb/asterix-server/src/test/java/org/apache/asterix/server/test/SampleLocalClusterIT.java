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

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.utils.Servlets;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.testframework.context.TestCaseContext.OutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SampleLocalClusterIT {

    // Important paths and files for this test.

    // The "target" subdirectory of asterix-server. All outputs go here.
    private static final String TARGET_DIR = joinPath("target");

    // Directory where the NCs create and store all data, as configured by
    // src/test/resources/NCServiceExecutionIT/cc.conf.
    private static final String OUTPUT_DIR = joinPath(TARGET_DIR, "sample local cluster");

    private static final String LOCAL_SAMPLES_DIR = joinPath(OUTPUT_DIR, "opt", "local");

    @Rule
    public TestRule watcher = new TestMethodTracer();

    @BeforeClass
    public static void setUp() throws Exception {
        // Create actual-results output directory.
        File outDir = new File(OUTPUT_DIR);

        // Remove any instance data from previous runs.
        if (outDir.isDirectory()) {
            FileUtils.deleteDirectory(outDir);
        }
        outDir.mkdirs();

        String[] pathElements = new String[] { TARGET_DIR,
                new File(TARGET_DIR).list((dir, name) -> name.matches("asterix-server.*-binary-assembly.zip"))[0] };
        String installerZip = joinPath(pathElements);

    }

    private static List<File> findLogFiles(File directory, List<File> fileList) {
        File [] match = directory.listFiles(pathname -> pathname.isDirectory() || pathname.toString().endsWith(".log"));
        if (match != null) {
            for (File file : match) {
                if (file.isDirectory()) {
                    findLogFiles(file, fileList);
                } else {
                    fileList.add(file);
                }
            }
        }
        return fileList;
    }

    @AfterClass
    public static void teardown() throws Exception {

        File destDir = new File(TARGET_DIR, joinPath("failsafe-reports", SampleLocalClusterIT.class.getSimpleName()));

        for (File f : findLogFiles(new File(OUTPUT_DIR), new ArrayList<>())) {
            FileUtils.copyFileToDirectory(f, destDir);
        }
    }

    @Test
    public void test0_startCluster() throws Exception {
        Process process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/stop-sample-cluster.sh"), "-f")
                .inheritIO().start();
        Assert.assertEquals(0, process.waitFor());
        process = new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/start-sample-cluster.sh")).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());
    }

    @Test
    public void test1_sanityQuery() throws Exception {
        TestExecutor testExecutor = new TestExecutor();
        InputStream resultStream = testExecutor.executeQuery("1+1", OutputFormat.ADM,
                new URI("http", null, "127.0.0.1", 19002, Servlets.AQL_QUERY, null, null),
                Collections.emptyList());
        StringWriter sw = new StringWriter();
        IOUtils.copy(resultStream, sw);
        Assert.assertEquals("2", sw.toString().trim());
    }

    @Test
    public void test2_stopCluster() throws Exception {
        Process process =
                new ProcessBuilder(joinPath(LOCAL_SAMPLES_DIR, "bin/stop-sample-cluster.sh")).inheritIO().start();
        Assert.assertEquals(0, process.waitFor());
        try {
            new URL("http://127.0.0.1:19002").openConnection().connect();
            Assert.assertTrue("Expected connection to be refused.", false);
        } catch (IOException e) {
            // expected
        }
    }
}
