/*
 * Copyright 2009-2014 by The Regents of the University of California
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
package edu.uci.ics.asterix.feeds;

import java.io.PrintWriter;
import java.io.StringReader;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.bootstrap.FeedBootstrap;
import edu.uci.ics.asterix.common.config.AsterixFeedProperties;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.MessageReceiver;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.ICentralFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage.MessageType;
import edu.uci.ics.asterix.metadata.feeds.MessageListener;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;

public class CentralFeedManager implements ICentralFeedManager {

    private static final Logger LOGGER = Logger.getLogger(CentralFeedManager.class.getName());

    private static ICentralFeedManager centralFeedManager = new CentralFeedManager();

    private static IFeedLoadManager feedLoadManager = new FeedLoadManager();

    private static int port;

    public static ICentralFeedManager getInstance() {
        return centralFeedManager;
    }

    private LinkedBlockingQueue<String> inbox = new LinkedBlockingQueue<String>();
    private final MessageListener messageListener;
    private final MessageReceiver<String> messageReceiver;

    private CentralFeedManager() {
        AsterixFeedProperties feedProperties = AsterixAppContextInfo.getInstance().getFeedProperties();
        port = feedProperties.getFeedCentralManagerPort();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Central Feed Manager will start message service at " + port);
        }
        messageListener = new MessageListener(port, inbox);
        messageReceiver = new FeedMessageReceiver(inbox);
    }

    @Override
    public void start() throws AsterixException {
        messageListener.start();
        messageReceiver.start();
    }

    @Override
    public void stop() throws AsterixException {
        messageListener.stop();
        messageReceiver.close(false);
    }

    private static class FeedMessageReceiver extends MessageReceiver<String> {

        private static boolean initialized;

        public FeedMessageReceiver(LinkedBlockingQueue<String> inbox) {
            super(inbox);
            initialized = false;
        }

        @Override
        public void processMessage(String message) throws Exception {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Received message:" + message);
            }
            JSONObject obj = new JSONObject(message);
            MessageType messageType = MessageType.valueOf(obj.getString(IFeedMessage.Constants.MESSAGE_TYPE));
            switch (messageType) {
                case XAQL:
                    if (!initialized) {
                        FeedBootstrap.setUpInitialArtifacts();
                        initialized = true;
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Created artifacts to store feed failure data");
                        }
                    }
                    String aql = obj.getString("aql");
                    AQLExecutor.executeAQL(aql);
                    break;
                case CONGESTION:
                    FeedCongestionMessage congestionMessage = FeedCongestionMessage.read(obj);
                    feedLoadManager.reportCongestion(congestionMessage);
                    break;
                case FEED_REPORT:
                    feedLoadManager.submitFeedRuntimeReport(obj);
                    break;
                case NODE_REPORT:
                    NodeLoadReport r = new NodeLoadReport(obj.getString(IFeedMessage.Constants.NODE_ID),
                            (float) obj.getDouble(IFeedMessage.Constants.CPU_LOAD));
                    feedLoadManager.submitNodeLoadReport(r);
                    break;
                default:
                    break;
            }

        }
    }

    public static class AQLExecutor {

        private static final PrintWriter out = new PrintWriter(System.out, true);

        public static void executeAQL(String aql) throws Exception {
            AQLParser parser = new AQLParser(new StringReader(aql));
            List<Statement> statements;
            statements = parser.Statement();
            SessionConfig pc = new SessionConfig(true, false, false, false, false, false, true, true, false);
            AqlTranslator translator = new AqlTranslator(statements, out, pc, DisplayFormat.TEXT);
            translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, false);
        }
    }

    public static IFeedLoadManager getFeedLoadManager() {
        return feedLoadManager;
    }
}
