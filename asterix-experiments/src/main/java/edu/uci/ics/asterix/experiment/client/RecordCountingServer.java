package edu.uci.ics.asterix.experiment.client;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RecordCountingServer {

    private final ExecutorService threadPool;

    private final int port;

    private final long duration;

    private ServerSocket ss;

    private boolean stopped;

    public RecordCountingServer(int port, long duration) {
        this.port = port;
        this.duration = duration;
        threadPool = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    stopped = false;
                    ss = new ServerSocket(port);
                    while (true) {
                        Socket s = ss.accept();
                        if (stopped) {
                            break;
                        }
                        threadPool.execute(new RecordCountingThread(s, duration));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.start();
    }

    public void stop() throws IOException, InterruptedException {
        stopped = true;
        ss.close();
        threadPool.shutdown();
        threadPool.awaitTermination(1000, TimeUnit.DAYS);
    }

    private static class RecordCountingThread implements Runnable {
        private final Socket s;

        private final long duration;

        private final char[] buf;

        private int index;

        private int count;

        public RecordCountingThread(Socket s, long duration) {
            this.s = s;
            this.duration = duration;
            buf = new char[32 * 1024];
        }

        @Override
        public void run() {
            count = 0;
            index = 0;
            long start = System.currentTimeMillis();
            try {
                InputStreamReader r = new InputStreamReader(s.getInputStream());
                while (System.currentTimeMillis() - start < duration) {
                    fill(r);
                    countRecords();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
            System.out.println("Read " + count + " records in " + (end - start) / 1000 + " seconds");
        }

        private void countRecords() {
            for (int i = 0; i < index; ++i) {
                if (buf[i] == '\n') {
                    ++count;
                }
            }
        }

        private void fill(Reader r) throws IOException {
            index = 0;
            int read = r.read(buf);
            if (read == -1) {
                index = 0;
                return;
            }
            index += read;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Expecting <duration> argument (unit = milliseconds)");
            System.exit(1);
        }
        long duration = Long.parseLong(args[0]);
        RecordCountingServer rcs = new RecordCountingServer(10005, duration);
        rcs.start();
        Thread.sleep(duration + 10000);
        rcs.stop();
    }
}
