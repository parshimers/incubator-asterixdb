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
package edu.uci.ics.asterix.installer.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;
import java.util.List;
import java.io.InputStream;
import java.io.FilenameFilter;
import java.util.Map;
import java.lang.ProcessBuilder;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import org.apache.commons.io.IOUtils;

import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

public class AsterixClusterLifeCycleIT {

    private static final String PATH_BASE = "src/test/resources/integrationts/lifecycle";
    private static final String CLUSTER_BASE = "src/test/resources/clusterts";
    private static final String VAGRANT_RC = "/home/imaxon/Work/asterixdb-vm/asterix-installer/target/vagrant/vagrantrc";
    private static final String PATH_ACTUAL = "ittest/";
    private static String managixFolderName = "";
    private static final Logger LOGGER = Logger.getLogger(AsterixClusterLifeCycleIT.class.getName());
    private static List<TestCaseContext> testCaseCollection;
    private static File asterixProjectDir = new File(System.getProperty("user.dir"));

    @BeforeClass
    public static void setUp() throws Exception {
        //testcase setup
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        testCaseCollection = b.build(new File(PATH_BASE));
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        //vagrant setup
        File installerTargetDir = new File(asterixProjectDir, "target");
        System.out.println(managixFolderName);
        managixFolderName = installerTargetDir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return new File(dir, name).isDirectory() && name.startsWith("asterix-installer")
                        && name.endsWith("binary-assembly");
            }

        })[0];
        invoke("cp","-r",installerTargetDir.toString()+"/"+managixFolderName,asterixProjectDir+"/"+CLUSTER_BASE);

        printOutput(remoteInvoke("cp -rv /vagrant/"+managixFolderName+" /tmp/asterix").getInputStream());

        printOutput(managixInvoke("configure").getInputStream());
        printOutput(managixInvoke("validate").getInputStream());

        Process p = managixInvoke("create -n vagrant-ssh -c /vagrant/cluster.xml");
        assert(checkOutput(p.getInputStream(),"ACTIVE"));
        LOGGER.info("Test start active cluster instance PASSED");

        printOutput(managixInvoke("stop -n vagrant-ssh").getInputStream());

    }

    @AfterClass
    public static void tearDown() throws Exception {
        Process p = managixInvoke("delete -n vagrant-ssh");
        assert (checkOutput(p.getInputStream(),"Deleted Asterix instance"));
        managixInvoke("rm -rf /vagrant/managix-working");
        LOGGER.info("Test delete active instance PASSED");
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        return testArgs;
    }
    public static boolean checkOutput(InputStream input, String requiredSubString){
        //right now im just going to look at the output, which is wholly inadequate
        //TODO: try using cURL to actually poke the instance to see if it is more alive
        String candidate = "";
        try{
            candidate = IOUtils.toString(input,"UTF-8");
        }
        catch(IOException e){
            return false;
        }
        //debug
        System.out.println(candidate);
        return candidate.contains(requiredSubString);
    }
    public static void printOutput(InputStream input){
        String candidate = "";
        try{
            candidate = IOUtils.toString(input,"UTF-8");
        }
        catch(IOException e){
        }
        //debug
        System.out.println(candidate);
    }

    private static Process invoke(String... args) throws Exception{
        ProcessBuilder pb = new ProcessBuilder(args);
        Map<String,String> env = pb.environment();
        pb.redirectErrorStream(true);
        Process p = pb.start();
        return p;
    }
    private static Process remoteInvoke(String cmd) throws Exception{
        ProcessBuilder pb = new ProcessBuilder("vagrant","ssh","cc", "-c","MANAGIX_HOME=/tmp/asterix/ "+cmd);
        System.out.println("---- "+cmd);
        Map<String,String> env = pb.environment();
        File cwd = new File(asterixProjectDir.toString()+"/"+CLUSTER_BASE);
        pb.directory(cwd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        return p;
    }
    private static Process managixInvoke(String cmd) throws Exception{
        return remoteInvoke("/tmp/asterix/bin/managix "+cmd);
    }

    @Test
    public void test1StartActiveInstance() throws Exception {
        //TODO: is the instance actually live?
        try {
                Process p = managixInvoke("start -n vagrant-ssh");
                assert(checkOutput(p.getInputStream(),"ACTIVE"));
                LOGGER.info("Test start active cluster instance PASSED");
            }
            catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    @Test
    public void test2StopActiveInstance() throws Exception {
        //TODO: is ZK still running?
        try {
            Process p = managixInvoke("stop -n vagrant-ssh");
            assert(checkOutput(p.getInputStream(),"Stopped Asterix instance"));
            LOGGER.info("Test stop active cluster instance PASSED");

        } catch (Exception e) {
            throw new Exception("Test configure installer " + "\" FAILED!", e);
        }
    }

    public void test() throws Exception {
        for (TestCaseContext testCaseCtx : testCaseCollection) {
            TestsUtils.executeTest(PATH_ACTUAL, testCaseCtx, null, false);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
            new AsterixClusterLifeCycleIT().test();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.info("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }

}
