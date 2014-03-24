package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.uci.ics.asterix.common.config.AsterixFeedProperties;

/**
 * Sends feed report messages on behalf of an operator instance
 * to the SuperFeedManager associated with the feed.
 */
public class FeedMessageService implements IFeedMessageService {

    private static final Logger LOGGER = Logger.getLogger(FeedMessageService.class.getName());

    private final String nodeId;
    private final LinkedBlockingQueue<String> inbox;
    private final FeedMessageHandler mesgHandler;
    private Executor executor;

    public FeedMessageService(String nodeId, AsterixFeedProperties feedProperties, String ccClusterIp) {
        this.nodeId = nodeId;
        this.inbox = new LinkedBlockingQueue<String>();
        this.mesgHandler = new FeedMessageHandler(inbox, ccClusterIp, feedProperties.getFeedCentralManagerPort());
        this.executor = Executors.newSingleThreadExecutor();
    }

    public void start() throws Exception {
        executor.execute(mesgHandler);
    }

    public void stop() {
        mesgHandler.stop();
    }

    @Override
    public void sendMessage(IFeedMessage message) {
        try {
            inbox.add(message.toJSON());
        } catch (JSONException jse) {

        }
    }

    private static class FeedMessageHandler implements Runnable {

        private final LinkedBlockingQueue<String> inbox;
        private Socket cfmSocket;
        private boolean process = true;
        private final String host;
        private int port;

        private static byte[] EOL = "\n".getBytes();

        public FeedMessageHandler(LinkedBlockingQueue<String> inbox, String host, int port) {
            this.inbox = inbox;
            this.host = host;
            this.port = port;
        }

        public void run() {
            try {
                cfmSocket = new Socket(host, port);
                if (cfmSocket != null) {
                    while (process) {
                        String message = inbox.take();
                        cfmSocket.getOutputStream().write(message.getBytes());
                        cfmSocket.getOutputStream().write(EOL);
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info(" Sent message " + message + " to CentralFeedManager ");
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to start feed message service");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception in handling incoming feed messages" + e.getMessage());
                }
            } finally {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Stopping feed message handler");
                }
                if (cfmSocket != null) {
                    try {
                        cfmSocket.close();
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Exception in closing socket " + e.getMessage());
                        }
                    }
                }
            }

        }

        public void stop() {
            process = false;
        }

    }

}
