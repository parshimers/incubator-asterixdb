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
package edu.uci.ics.asterix.metadata.feeds;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.CharBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MessageListener {

    private static final Logger LOGGER = Logger.getLogger(MessageListener.class.getName());

    private final int port;
    private final LinkedBlockingQueue<String> outbox;

    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    private MessageListenerServer listenerServer;

    public MessageListener(int port, LinkedBlockingQueue<String> outbox) {
        this.port = port;
        this.outbox = outbox;
    }

    public void stop() {
        listenerServer.stop();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Stopped message service at " + port);
        }
        if (!executorService.isShutdown()) {
            executorService.shutdownNow();
        }

    }

    public void start() {
        try {
            listenerServer = new MessageListenerServer(port, outbox);
            executorService.execute(listenerServer);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Starting message service at " + port);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to start message listener server");
            }
        }
    }

    private static class MessageListenerServer implements Runnable {

        private final int port;
        private final LinkedBlockingQueue<String> outbox;
        private ServerSocket server;
        private final Executor executor;

        public MessageListenerServer(int port, LinkedBlockingQueue<String> outbox) {
            this.port = port;
            this.outbox = outbox;
            this.executor = Executors.newCachedThreadPool();
        }

        public void stop() {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            Socket client = null;
            try {
                server = new ServerSocket(port);
                while (true) {
                    client = server.accept();
                    ClientHandler handler = new ClientHandler(client, outbox);
                    executor.execute(handler);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Received connection request from " + client);
                    }
                }

            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unable to start Message listener" + server);
                }
            } finally {
                if (server != null) {
                    try {
                        server.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        private static class ClientHandler implements Runnable {

            private final Socket client;
            private final LinkedBlockingQueue<String> outbox;
            private static char EOL = (char) "\n".getBytes()[0];

            public ClientHandler(Socket client, LinkedBlockingQueue<String> outbox) {
                this.client = client;
                this.outbox = outbox;
            }

            @Override
            public void run() {
                try {
                    InputStream in = client.getInputStream();
                    CharBuffer buffer = CharBuffer.allocate(5000);
                    char ch;
                    while (true) {
                        ch = (char) in.read();
                        if (((int) ch) == -1) {
                            break;
                        }
                        while (ch != EOL) {
                            buffer.put(ch);
                            ch = (char) in.read();
                        }
                        buffer.flip();
                        String s = new String(buffer.array());
                        synchronized (outbox) {
                            outbox.add(s + "\n");
                        }
                        buffer.position(0);
                        buffer.limit(5000);
                    }
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to process mesages from client" + client);
                    }
                } finally {
                    if (client != null) {
                        try {
                            client.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

    }
}