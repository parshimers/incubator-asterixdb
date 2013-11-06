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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IFeedAdapter;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;

public class AdapterRuntimeManager implements IAdapterRuntimeManager {

    private static final Logger LOGGER = Logger.getLogger(AdapterRuntimeManager.class.getName());

    private final FeedId feedId;

    private IFeedAdapter feedAdapter;

    private AdapterExecutor adapterExecutor;

    private State state;

    private int partition;

    private IngestionRuntime ingestionRuntime;

    public enum State {
        /*
         * Indicates that data from external source will be pushed downstream
         * for storage
         */
        ACTIVE_INGESTION,
        /*
         * Indicates that data from external source would be buffered and not
         * pushed downstream
         */
        INACTIVE_INGESTION,
        /*
         * Indicates that feed ingestion activity has finished
         */
        FINISHED_INGESTION
    }

    public AdapterRuntimeManager(FeedId feedId, IFeedAdapter feedAdapter, DistributeFeedFrameWriter writer,
            int partition) {
        this.feedId = feedId;
        this.feedAdapter = feedAdapter;
        this.partition = partition;
        this.adapterExecutor = new AdapterExecutor(partition, writer, feedAdapter, this);
        this.state = State.INACTIVE_INGESTION;
    }

    @Override
    public void start() throws Exception {
        state = State.ACTIVE_INGESTION;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registered feed runtime manager for " + this.getFeedId());
        }
        (new Thread(adapterExecutor)).start();
    }

    @Override
    public void stop() {
        try {
            feedAdapter.stop();
            state = State.FINISHED_INGESTION;
            //   ingestionRuntime.endOfFeed();
        } catch (Exception exception) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe("Unable to stop adapter " + feedAdapter + ", encountered exception " + exception);
            }
        }
    }

    @Override
    public FeedId getFeedId() {
        return feedId;
    }

    @Override
    public String toString() {
        return feedId + "[" + partition + "]";
    }

    public IFeedAdapter getFeedAdapter() {
        return feedAdapter;
    }

    public void setFeedAdapter(IFeedAdapter feedAdapter) {
        this.feedAdapter = feedAdapter;
    }

    public static class AdapterExecutor implements Runnable {

        private DistributeFeedFrameWriter writer;

        private IFeedAdapter adapter;

        private AdapterRuntimeManager runtimeManager;

        public AdapterExecutor(int partition, DistributeFeedFrameWriter writer, IFeedAdapter adapter,
                AdapterRuntimeManager adapterRuntimeMgr) {
            this.writer = writer;
            this.adapter = adapter;
            this.runtimeManager = adapterRuntimeMgr;
        }

        @Override
        public void run() {
            try {
                int partition = runtimeManager.getPartition();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Starting ingestion for partition:" + partition);
                }
                adapter.start(partition, writer);
                runtimeManager.setState(State.FINISHED_INGESTION);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Exception during feed ingestion " + e.getMessage());
                    e.printStackTrace();
                }
            } finally {
                synchronized (runtimeManager) {
                    runtimeManager.notifyAll();
                }
            }
        }

        public DistributeFeedFrameWriter getWriter() {
            return writer;
        }

        public void setWriter(IFrameWriter writer) {
            if (this.writer != null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Switching writer to:" + writer + " from " + this.writer);
                }
            }
        }

    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
    }

    public AdapterExecutor getAdapterExecutor() {
        return adapterExecutor;
    }

    public int getPartition() {
        return partition;
    }

    public IngestionRuntime getIngestionRuntime() {
        return ingestionRuntime;
    }

}
