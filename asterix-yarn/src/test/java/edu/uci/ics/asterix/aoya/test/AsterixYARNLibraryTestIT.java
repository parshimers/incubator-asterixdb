package edu.uci.ics.asterix.aoya.test;

import java.util.logging.Logger;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.uci.ics.asterix.aoya.AsterixYARNClient;

public class AsterixYARNLibraryTestIT {
    private static final String INSTANCE_NAME = "asterix-lib-test";
    private static final String PATH_ACTUAL = "ittest/";
    private static final Logger LOGGER = Logger.getLogger(AsterixYARNLifecycleIT.class.getName());
    private static String configPath;
    private static String aoyaServerPath;
    private static String parameterPath;
    private static AsterixYARNInstanceUtil instance;
    private static YarnConfiguration appConf;

    @BeforeClass
    public static void setUp() throws Exception {
        instance = new AsterixYARNInstanceUtil();
        appConf = instance.setUp();
        configPath = instance.configPath;
        aoyaServerPath = instance.aoyaServerPath;
        parameterPath = instance.parameterPath;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        instance.tearDown();
    }

    @Test
    public void test_1_InstallActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " -c " + configPath + " -bc " + parameterPath + " -zip "
                + aoyaServerPath + " install";
        executeAoyaCommand(command);
    }

    @Test
    public void test_2_StopActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " stop";
        executeAoyaCommand(command);
    }

    @Test
    public static void test_3_libInstall() throws Exception {

    }

    @Test
    public void test_4_StartActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " start";
        executeAoyaCommand(command);
    }

    @Test
    public void test_5_DeleteActiveInstance() throws Exception {
        String command = "-n " + INSTANCE_NAME + " -zip " + aoyaServerPath + " -f" + " destroy";
        executeAoyaCommand(command);
    }
    static void executeAoyaCommand(String cmd) throws Exception {
        AsterixYARNClient aoyaClient = new AsterixYARNClient(appConf);
        aoyaClient.init(cmd.split(" "));
        AsterixYARNClient.execute(aoyaClient);
    }
}
