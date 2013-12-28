package edu.uci.ics.asterix.common.feeds;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class MessageReceiver<T> implements IMessageReceiver<T> {

    protected static final Logger LOGGER = Logger.getLogger(MessageReceiver.class.getName());

    private final LinkedBlockingQueue<T> inbox;
    private ExecutorService executor;

    public MessageReceiver() {
        inbox = new LinkedBlockingQueue<T>();
    }

    public abstract void processMessage(T message) throws Exception;

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new MessageReceiverRunnable<T>(this));
    }

    @Override
    public void sendMessage(T message) {
        inbox.add(message);
    }

    @Override
    public void close(boolean processPending) {
        if (executor != null) {
            executor.shutdown();
            executor = null;
            flushPendingMessages();
        }
    }

    private static class MessageReceiverRunnable<T> implements Runnable {

        private final LinkedBlockingQueue<T> inbox;
        private final MessageReceiver<T> messageReceiver;

        public MessageReceiverRunnable(MessageReceiver messageReceiver) {
            this.inbox = messageReceiver.inbox;
            this.messageReceiver = messageReceiver;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    T message = inbox.take();
                    messageReceiver.processMessage(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e) {

                }
            }
        }
    }

    protected void flushPendingMessages() {
        while (!inbox.isEmpty()) {
            T message = null;
            try {
                message = inbox.take();
                processMessage(message);
            } catch (InterruptedException ie) {
                // ignore
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception " + e + " in processing message " + message);
                }
            }
        }
    }

}
