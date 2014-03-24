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
import edu.uci.ics.asterix.common.feeds.ICentralFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedMessage.MessageType;
import edu.uci.ics.asterix.common.feeds.MessageReceiver;
import edu.uci.ics.asterix.metadata.feeds.MessageListener;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;

public class CentralFeedManager implements ICentralFeedManager {

    private static final Logger LOGGER = Logger.getLogger(CentralFeedManager.class.getName());

    private static ICentralFeedManager centralFeedManager = new CentralFeedManager();

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
            if (!initialized) {
                FeedBootstrap.setUpInitialArtifacts();
                initialized = true;
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Created artifacts to store feed failure data");
                }
            }
            JSONObject obj = new JSONObject(message);
            MessageType messageType = MessageType.valueOf(obj.getString("message-type"));
            switch (messageType) {
                case XAQL:
                    String aql = obj.getString("aql");
                    AQLExecutor.executeAQL(aql);
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
}
