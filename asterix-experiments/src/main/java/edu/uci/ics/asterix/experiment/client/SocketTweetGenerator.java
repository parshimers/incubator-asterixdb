package edu.uci.ics.asterix.experiment.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.asterix.tools.external.data.TweetGenerator;

public class SocketTweetGenerator {

    private final ExecutorService threadPool;

    private final int partitionRangeStart;

    private final int duration;

    private final long startDataInterval;

    private final int nDataIntervals;

    private final String orchHost;

    private final int orchPort;

    private final List<Pair<String, Integer>> receiverAddresses;

    private final Mode mode;

    private enum Mode {
        TIME,
        DATA
    }

    public SocketTweetGenerator(SocketTweetGeneratorConfig config) {
        threadPool = Executors.newCachedThreadPool(new ThreadFactory() {

            private final AtomicInteger count = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                int tid = count.getAndIncrement();
                Thread t = new Thread(r, "DataGeneratorThread: " + tid);
                t.setDaemon(true);
                return t;
            }
        });

        partitionRangeStart = config.getPartitionRangeStart();
        duration = config.getDuration();
        startDataInterval = config.getDataInterval();
        nDataIntervals = config.getNIntervals();
        orchHost = config.getOrchestratorHost();
        orchPort = config.getOrchestratorPort();
        receiverAddresses = config.getAddresses();
        mode = startDataInterval > 0 ? Mode.DATA : Mode.TIME;
    }

    public void start() throws Exception {
        final Semaphore sem = new Semaphore((receiverAddresses.size() - 1) * -1);
        int i = 0;
        for (Pair<String, Integer> address : receiverAddresses) {
            threadPool.submit(new DataGenerator(mode, sem, address.getLeft(), address.getRight(), i
                    + partitionRangeStart, duration, nDataIntervals, startDataInterval, orchHost, orchPort));
            ++i;
        }
        sem.acquire();
    }

    public class DataGenerator implements Runnable {

        private final Mode m;
        private final Semaphore sem;
        private final String host;
        private final int port;
        private final int partition;
        private final int duration;
        private final int nDataIntervals;
        private final String orchHost;
        private final int orchPort;

        private int currentInterval;
        private long nextStopInterval;

        public DataGenerator(Mode m, Semaphore sem, String host, int port, int partition, int duration,
                int nDataIntervals, long initialDataSizeInterval, String orchHost, int orchPort) {
            this.m = m;
            this.sem = sem;
            this.host = host;
            this.port = port;
            this.partition = partition;
            this.duration = duration;
            this.nDataIntervals = nDataIntervals;
            currentInterval = 0;
            this.nextStopInterval = initialDataSizeInterval;
            this.orchHost = orchHost;
            this.orchPort = orchPort;
        }

        @Override
        public void run() {
            try {
                Socket s = new Socket(host, port);
                try {
                    Socket orchSocket = null;
                    if (m == Mode.DATA) {
                        orchSocket = new Socket(orchHost, orchPort);
                    }
                    try {
                        Map<String, String> config = new HashMap<>();
                        String durationVal = m == Mode.TIME ? String.valueOf(duration) : "0";
                        config.put(TweetGenerator.KEY_DURATION, String.valueOf(durationVal));

                        TweetGenerator tg = new TweetGenerator(config, partition, s.getOutputStream());
                        while (tg.setNextRecordBatch(1000)) {
                            if (m == Mode.DATA) {
                                if (tg.getFlushedDataSize() >= nextStopInterval) {
                                    // send stop to orchestrator
                                    sendStopped(orchSocket);

                                    // update intervals
                                    nextStopInterval *= 2;
                                    if (++currentInterval >= nDataIntervals) {
                                        break;
                                    }

                                    receiveResume(orchSocket);
                                }
                            }
                        }
                        System.out.println("Num tweets flushed = " + tg.getNumFlushedTweets() + " in " + duration
                                + " seconds.");
                    } finally {
                        if (orchSocket != null) {
                            orchSocket.close();
                        }
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                } finally {
                    s.close();
                }
            } catch (Throwable t) {
                System.err.println("Error connecting to " + host + ":" + port);
                t.printStackTrace();
            } finally {
                sem.release();
            }
        }

        private void receiveResume(Socket s) throws IOException {
            int msg = new DataInputStream(s.getInputStream()).readInt();
            OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
            if (msgType != OrchestratorDGProtocol.RESUME) {
                throw new IllegalStateException("Unknown protocol message received: " + msgType);
            }
        }

        private void sendStopped(Socket s) throws IOException {
            new DataOutputStream(s.getOutputStream()).writeInt(OrchestratorDGProtocol.STOPPED.ordinal());
            s.getOutputStream().flush();
        }

    }

    private static class CircularByteArrayOutputStream extends OutputStream {

        private final byte[] buf;

        private int index;

        public CircularByteArrayOutputStream() {
            buf = new byte[32 * 1024];
            index = 0;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException {
            if (b == null) {
                throw new NullPointerException();
            } else if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return;
            }

            int remain = len;
            int remainOff = off;
            while (remain > 0) {
                int avail = buf.length - index;
                System.arraycopy(b, remainOff, buf, index, avail);
                remainOff += avail;
                remain -= avail;
                index = (index + avail) % buf.length;
            }
        }

        @Override
        public void write(int b) throws IOException {
            buf[index] = (byte) b;
            index = (index + 1) % buf.length;
        }

    }
}
