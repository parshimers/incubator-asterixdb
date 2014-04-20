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
package edu.uci.ics.asterix.external.adapter.factory;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter;
import edu.uci.ics.asterix.metadata.feeds.ITypedAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksCountPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * Factory class for creating an instance of PullBasedTwitterAdapter.
 * This adapter provides the functionality of fetching tweets from Twitter service
 * via pull-based Twitter API.
 */
public class PullBasedTwitterAdapterFactory implements ITypedAdapterFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(PullBasedTwitterAdapterFactory.class.getName());

    public static final String PULL_BASED_TWITTER_ADAPTER_NAME = "pull_twitter";

    // configuration parameters
    public static final String QUERY = "query";
    public static final String INTERVAL = "interval";
    public static final String OAUTH_CONSUMER_KEY = "consumer.key";
    public static final String OAUTH_CONSUMER_SECRET = "consumer.secret";
    public static final String OAUTH_ACCESS_TOKEN = "access.token";
    public static final String OAUTH_ACCESS_TOKEN_SECRET = "access.token.secret";
    public static final String OAUTH_AUTHENTICATION_FILE = "authentication.file";
    public static final String AUTHENTICATION_MODE = "authentication.mode";
    public static final String OUTPUT_TYPE_NAME = "output.type.name";

    public static final String AUTHENTICATION_MODE_FILE = "file";
    public static final String AUTHENTICATION_MODE_EXPLICIT = "explicit";

    private static final String TWITTER_USER_DATATYPE_NAME = "TwitterUser";
    private static final String TWEET_DATATYPE_NAME = "Tweet";

    private static final String DEFAULT_INTERVAL = "10"; // 10 seconds
    private static final String DEFAULT_AUTH_FILE = "/feed/twitter/auth.properties"; // default authentication file
    private static final int INTAKE_CARDINALITY = 1; // degree of parallelism at intake stage 

    private static ARecordType recordType = initOutputType();

    private Map<String, String> configuration;

    private static ARecordType initOutputType() {
        ARecordType recordType = null;
        try {
            String[] userFieldNames = { "screen-name", "lang", "friends_count", "statuses-count", "name",
                    "followers-count" };
            IAType[] userFieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                    BuiltinType.AINT32, BuiltinType.ASTRING, BuiltinType.AINT32 };
            ARecordType userType = new ARecordType(TWITTER_USER_DATATYPE_NAME, userFieldNames, userFieldTypes, false);

            String[] fieldNames = { "tweetid", "user", "location-lat", "location-long", "send-time", "message-text" };
            IAType[] fieldTypes = { BuiltinType.ASTRING, userType, BuiltinType.ADOUBLE, BuiltinType.ADOUBLE,
                    BuiltinType.ASTRING, BuiltinType.ASTRING };
            recordType = new ARecordType(TWEET_DATATYPE_NAME, fieldNames, fieldTypes, false);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create adapter output type");
        }
        return recordType;
    }

    @Override
    public IDatasourceAdapter createAdapter(IHyracksTaskContext ctx, int partition) throws Exception {
        return new PullBasedTwitterAdapter(configuration, recordType, ctx);
    }

    @Override
    public String getName() {
        return PULL_BASED_TWITTER_ADAPTER_NAME;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

    @Override
    public SupportedOperation getSupportedOperations() {
        return SupportedOperation.READ;
    }

    @Override
    public void configure(Map<String, String> configuration) throws Exception {
        this.configuration = configuration;
        String authMode = configuration.get(AUTHENTICATION_MODE);
        if (authMode == null) {
            authMode = AUTHENTICATION_MODE_FILE;
        }
        try {
            switch (authMode) {
                case AUTHENTICATION_MODE_FILE:
                    Properties prop = new Properties();
                    String authFile = configuration.get(OAUTH_AUTHENTICATION_FILE);
                    if (authFile == null) {
                        authFile = DEFAULT_AUTH_FILE;
                    }
                    InputStream in = getClass().getResourceAsStream(authFile);
                    prop.load(in);
                    in.close();
                    configuration.put(OAUTH_CONSUMER_KEY, prop.getProperty(OAUTH_CONSUMER_KEY));
                    configuration.put(OAUTH_CONSUMER_SECRET, prop.getProperty(OAUTH_CONSUMER_SECRET));
                    configuration.put(OAUTH_ACCESS_TOKEN, prop.getProperty(OAUTH_ACCESS_TOKEN));
                    configuration.put(OAUTH_ACCESS_TOKEN_SECRET, prop.getProperty(OAUTH_ACCESS_TOKEN_SECRET));
                    break;
                case AUTHENTICATION_MODE_EXPLICIT:
                    break;
            }
        } catch (Exception e) {
            throw new AsterixException("Incorrect configuration! unable to load authentication credentials "
                    + e.getMessage());
        }

        if (configuration.get(QUERY) == null) {
            throw new AsterixException("parameter " + QUERY + " not specified as part of adaptor configuration");
        }
        String interval = configuration.get(INTERVAL);
        if (interval != null) {
            try {
                Integer.parseInt(interval);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("parameter " + INTERVAL
                        + " is defined incorrectly, expecting a number");
            }
        } else {
            configuration.put(INTERVAL, DEFAULT_INTERVAL);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning(" Parameter " + INTERVAL + " not defined, using default (" + DEFAULT_INTERVAL + ")");
            }
        }

    }

    @Override
    public AlgebricksPartitionConstraint getPartitionConstraint() throws Exception {
        return new AlgebricksCountPartitionConstraint(INTAKE_CARDINALITY);
    }

    @Override
    public ARecordType getAdapterOutputType() {
        return recordType;
    }

}
