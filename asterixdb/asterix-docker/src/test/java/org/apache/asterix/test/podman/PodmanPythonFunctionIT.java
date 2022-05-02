package org.apache.asterix.test.podman;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.test.common.TestExecutor;
import org.apache.asterix.test.runtime.LangExecutionUtil;
import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the SQLPP runtime tests with the storage parallelism.
 */
@RunWith(Parameterized.class)
public class PodmanPythonFunctionIT {
    public static final DockerImageName ASTERIX_IMAGE = DockerImageName.parse("asterixdb/socktest");
    @ClassRule
    public static GenericContainer<?> asterix = new GenericContainer(ASTERIX_IMAGE).withExposedPorts(19004,5006).withFileSystemBind("../asterix-app/","/var/tmp/asterix-app/", BindMode.READ_WRITE);
    protected static final String TEST_CONFIG_FILE_NAME = "../asterix-app/src/test/resources/cc.conf";

    @BeforeClass
    public static void setUp() throws Exception {
        final TestExecutor testExecutor = new TestExecutor();
        //NO.
        Thread.sleep(5000);
        Container.ExecResult res = asterix.execInContainer("/bin/sh", "-c", "cd /var/tmp/asterix-app/ && shiv -o target/TweetSent.pyz --site-packages src/test/resources/TweetSent scikit-learn");
        Container.ExecResult res2 = asterix.execInContainer("cp", "-a", "/var/tmp/asterix-app/data/classifications", "/opt/apache-asterixdb/data/"  );
        Container.ExecResult res3 = asterix.execInContainer("cp", "-a", "/var/tmp/asterix-app/data/twitter", "/opt/apache-asterixdb/data/"  );
        Container.ExecResult res4 = asterix.execInContainer("cp", "-a", "/var/tmp/asterix-app/data/big-object", "/opt/apache-asterixdb/data/"  );
        Container.ExecResult res5 = asterix.execInContainer("mkdir", "-p", "/opt/apache-asterixdb/target/data/"  );
        Container.ExecResult res6 = asterix.execInContainer("cp", "-a", "/var/tmp/asterix-app/target/data/big-object", "/opt/apache-asterixdb/target/data/"  );
        LangExecutionUtil.setUp(TEST_CONFIG_FILE_NAME, testExecutor, false, true, new PodmanUDFLibrarian(asterix));
        setNcEndpoints(testExecutor);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LangExecutionUtil.tearDown();
    }

    @Parameters(name = "PodmanPythonFunctionIT {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return LangExecutionUtil.tests("only_sqlpp.xml", "testsuite_it_python.xml" ,"../asterix-app/src/test/resources/runtimets");
    }

    protected TestCaseContext tcCtx;

    public PodmanPythonFunctionIT(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        LangExecutionUtil.test(tcCtx);
    }

    private static void setNcEndpoints(TestExecutor testExecutor) {
        final Map<String, InetSocketAddress> ncEndPoints = new HashMap<>();
        final String ip = asterix.getHost();
        //!!HACK
        final String nodeId = "asterix_nc";
        int apiPort = asterix.getFirstMappedPort();
        ncEndPoints.put(nodeId, InetSocketAddress.createUnresolved(ip, apiPort));
        testExecutor.setNcEndPoints(ncEndPoints);
    }
}
