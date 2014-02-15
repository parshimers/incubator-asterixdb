package edu.uci.ics.asterix.experiment.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.uci.ics.asterix.experiment.action.base.IAction;

public class OrchestratorServer {

    private final int port;

    private final int nDataGens;

    private final int nIntervals;

    private final AtomicBoolean running;

    private final IAction pauseAction;

    public OrchestratorServer(int port, int nDataGens, int nIntervals, IAction pauseAction) {
        this.port = port;
        this.nDataGens = nDataGens;
        this.nIntervals = nIntervals;
        running = new AtomicBoolean();
        this.pauseAction = pauseAction;
    }

    public synchronized void start() throws IOException, InterruptedException {
        final AtomicBoolean bound = new AtomicBoolean();
        running.set(true);
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    ServerSocket ss = new ServerSocket(port);
                    synchronized (bound) {
                        bound.set(true);
                        bound.notifyAll();
                    }
                    Socket[] conn = new Socket[nDataGens];
                    try {
                        for (int i = 0; i < nDataGens; i++) {
                            conn[i] = ss.accept();
                        }
                        for (int n = 0; n < nIntervals; ++n) {
                            for (int i = 0; i < nDataGens; i++) {
                                receiveStopped(conn[i]);
                            }
                            pauseAction.perform();
                            if (n != nIntervals - 1) {
                                for (int i = 0; i < nDataGens; i++) {
                                    sendResume(conn[i]);
                                }
                            }
                        }
                    } finally {
                        for (int i = 0; i < conn.length; ++i) {
                            if (conn[i] != null) {
                                conn[i].close();
                            }
                        }
                        ss.close();
                    }
                    running.set(false);
                    synchronized (OrchestratorServer.this) {
                        OrchestratorServer.this.notifyAll();
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        });
        t.start();
        synchronized (bound) {
            while (!bound.get()) {
                bound.wait();
            }
        }
    }

    private void sendResume(Socket conn) throws IOException {
        new DataOutputStream(conn.getOutputStream()).writeInt(OrchestratorDGProtocol.RESUME.ordinal());
        conn.getOutputStream().flush();
    }

    private void receiveStopped(Socket conn) throws IOException {
        int msg = new DataInputStream(conn.getInputStream()).readInt();
        OrchestratorDGProtocol msgType = OrchestratorDGProtocol.values()[msg];
        if (msgType != OrchestratorDGProtocol.STOPPED) {
            throw new IllegalStateException("Encounted unknown message type " + msgType);
        }
    }

    public synchronized void awaitFinished() throws InterruptedException {
        while (running.get()) {
            wait();
        }
    }

}
