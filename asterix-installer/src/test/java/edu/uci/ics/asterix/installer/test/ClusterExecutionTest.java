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
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.external.util.IdentitiyResolverFactory;
import edu.uci.ics.asterix.test.aql.ClusterTestsUtils;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class ClusterExecutionTest {

    private static final Logger LOGGER = Logger.getLogger(ClusterExecutionTest.class.getName());

    private static final String PATH_ACTUAL = "ittest" + File.separator;
    private static final String PATH_BASE = StringUtils.join(new String[] { "..", "asterix-app", "src", "test",
            "resources", "runtimets" }, File.separator);

    @BeforeClass
    public static void setUp() throws Exception {
        System.out.println("Starting setup");
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting setup");
        }
        System.setProperty(GlobalConfig.WEB_SERVER_PORT_PROPERTY, "19002");
        File outdir = new File(PATH_ACTUAL);
        outdir.mkdirs();

        AsterixClusterLifeCycleIT.setUp();

        // Set the node resolver to be the identity resolver that expects node names 
        // to be node controller ids; a valid assumption in test environment. 
        System.setProperty(FileSystemBasedAdapter.NODE_RESOLVER_FACTORY_PROPERTY,
                IdentitiyResolverFactory.class.getName());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        File outdir = new File(PATH_ACTUAL);
        File[] files = outdir.listFiles();
        if (files == null || files.length == 0) {
            outdir.delete();
        }
        AsterixClusterLifeCycleIT.tearDown();
    }

    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = new ArrayList<Object[]>();
        TestCaseContext.Builder b = new TestCaseContext.Builder();
        for (TestCaseContext ctx : b.build(new File(PATH_BASE))) {
            testArgs.add(new Object[] { ctx });
        }
        return testArgs;
    }

    private TestCaseContext tcCtx;

    public ClusterExecutionTest(TestCaseContext tcCtx) {
        this.tcCtx = tcCtx;
    }

    @Test
    public void test() throws Exception {
        ClusterTestsUtils.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
