package edu.uci.ics.asterix.metadata.feeds;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.api.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.api.IAdapterRuntimeManager.State;
import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;

public class AdapterExecutor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(AdapterExecutor.class.getName());
    private DistributeFeedFrameWriter writer;

    private IFeedAdapter adapter;

    private IAdapterRuntimeManager runtimeManager;

    public AdapterExecutor(int partition, DistributeFeedFrameWriter writer, IFeedAdapter adapter,
            IAdapterRuntimeManager adapterRuntimeMgr) {
        this.writer = writer;
        this.adapter = adapter;
        this.runtimeManager = adapterRuntimeMgr;
    }

    @Override
    public void run() {
        int partition = runtimeManager.getPartition();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting ingestion for partition:" + partition);
        }
        boolean continueIngestion = true;
        while (continueIngestion) {
            try {
                adapter.start(partition, writer);
                continueIngestion = false;
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception during feed ingestion " + e.getMessage());
                    e.printStackTrace();
                }
                continueIngestion = adapter.handleException(e);
            }
        }

        runtimeManager.setState(State.FINISHED_INGESTION);
        synchronized (runtimeManager) {
            runtimeManager.notifyAll();
        }
    }

    public DistributeFeedFrameWriter getWriter() {
        return writer;
    }

}