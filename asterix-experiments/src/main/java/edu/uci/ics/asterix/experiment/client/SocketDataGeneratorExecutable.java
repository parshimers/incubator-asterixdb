package edu.uci.ics.asterix.experiment.client;

import java.net.Socket;
import java.util.Collections;

import edu.uci.ics.asterix.experiment.action.base.AbstractAction;
import edu.uci.ics.asterix.tools.external.data.TweetGenerator;

public class SocketDataGeneratorExecutable extends AbstractAction {

    private final String adapterHost;

    private final int adapterPort;

    public SocketDataGeneratorExecutable(String adapterHost, int adapterPort) {
        this.adapterHost = adapterHost;
        this.adapterPort = adapterPort;
    }

    @Override
    protected void doPerform() throws Exception {
        Thread.sleep(4000);
        Socket s = new Socket(adapterHost, adapterPort);
        try {
            TweetGenerator tg = new TweetGenerator(Collections.<String, String> emptyMap(), 0, s.getOutputStream());
            long start = System.currentTimeMillis();
            while (tg.setNextRecordBatch(1000)) {
            }
            long end = System.currentTimeMillis();
            long total = end - start;
            System.out.println("Generation finished: " + tg.getNumFlushedTweets() + " in " + total / 1000 + " seconds");
        } finally {
            s.close();
        }
    }

}
