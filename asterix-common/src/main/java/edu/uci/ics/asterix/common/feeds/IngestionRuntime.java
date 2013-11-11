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
package edu.uci.ics.asterix.common.feeds;

import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FrameReader;

public class IngestionRuntime extends SubscribableRuntime {

    private final IAdapterRuntimeManager adapterRuntimeManager;

    public IngestionRuntime(FeedId feedId, int partition, IAdapterRuntimeManager adaptorRuntimeManager,
            DistributeFeedFrameWriter feedWriter) {
        super(new FeedSubscribableRuntimeId(feedId, partition), feedWriter, FeedRuntimeType.INGEST);
        this.adapterRuntimeManager = adaptorRuntimeManager;
    }

    public void subscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        FrameReader reader = feedWriter.subscribeFeed(collectionRuntime.getFrameWriter());
        collectionRuntime.setFrameReader(reader);
        if (feedWriter.getDistributionMode().equals(DistributeFeedFrameWriter.DistributionMode.SINGLE)) {
            adapterRuntimeManager.start();
        }
        subscribers.add(collectionRuntime);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed feed collection [" + collectionRuntime + "] to " + this);
        }
    }

    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        feedWriter.unsubscribeFeed(collectionRuntime.getFrameWriter());
        if (feedWriter.getDistributionMode().equals(DistributeFeedFrameWriter.DistributionMode.INACTIVE)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Stopping adapter for " + this);
            }
            adapterRuntimeManager.stop();
        }
        subscribers.remove(collectionRuntime);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Unsubscribed feed collection [" + collectionRuntime + "] from " + this);
        }
    }

    public void endOfFeed() {
        feedWriter.notifyEndOfFeed();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Notified End Of Feed  [" + this + "]");
        }
    }

    public IAdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

}
