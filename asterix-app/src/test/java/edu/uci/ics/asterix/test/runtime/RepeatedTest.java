package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import edu.uci.ics.asterix.api.common.AsterixHyracksIntegrationUtil;
import edu.uci.ics.asterix.common.config.AsterixPropertiesAccessor;
import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.test.runtime.RepeatRule.Repeat;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
class RepeatRule implements MethodRule {

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ java.lang.annotation.ElementType.METHOD })
    public @interface Repeat {
        public abstract int times();

    }

    private static class RepeatStatement extends Statement {

        private final int times;

        private final Statement statement;

        private RepeatStatement(int times, Statement statement) {
            this.times = times;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < times; i++) {
                statement.evaluate();
            }
        }
    }

    @Override
    public Statement apply(Statement statement, FrameworkMethod method, Object target) {
        Statement result = statement;
        Repeat repeat = method.getAnnotation(Repeat.class);
        if (repeat != null) {
            int times = repeat.times();
            result = new RepeatStatement(times, statement);
        }
        return result;

    }
}

@RunWith(Parameterized.class)
public class RepeatedTest {

    private static final Logger LOGGER = Logger.getLogger(RepeatedTest.class.getName());

    private static final String PATH_ACTUAL = "rttest" + File.separator;
    private static final String PATH_BASE = StringUtils.join(new String[] { "src", "test", "resources", "runtimets" },
            File.separator);

    private static final String TEST_CONFIG_FILE_NAME = "asterix-build-configuration.xml";

    private static AsterixTransactionProperties txnProperties;

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting setup");
        }
        System.setProperty(GlobalConfig.CONFIG_FILE_PROPERTY, TEST_CONFIG_FILE_NAME);
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        AsterixPropertiesAccessor apa = new AsterixPropertiesAccessor();
        txnProperties = new AsterixTransactionProperties(apa);

        deleteTransactionLogs();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing pseudo cluster");
        }
        AsterixHyracksIntegrationUtil.init();

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("initializing HDFS");
        }

        HDFSCluster.getInstance().setup();

        // Set the node resolver to be the identity resolver that expects node names 
        // to be node controller ids; a valid assumption in test environment. 
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        AsterixHyracksIntegrationUtil.deinit();
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        // clean up the files written by the ASTERIX storage manager
        for (String d : AsterixHyracksIntegrationUtil.getDataDirs()) {
            TestsUtils.deleteRec(new File(d));
        }
        HDFSCluster.getInstance().cleanup();
    }

    private static void deleteTransactionLogs() throws Exception {
        for (String ncId : AsterixHyracksIntegrationUtil.getNcNames()) {
            File log = new File(txnProperties.getLogDirectory(ncId));
            if (log.exists()) {
                FileUtils.deleteDirectory(log);
            }
        }
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE), TestCaseContext.DEFAULT_REPEADED_TESTSUITE_XML_NAME)) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    private TestCaseContext tcCtx;

    public RepeatedTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = 10000)
    public void test() throws Exception {
        TestsUtils.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
