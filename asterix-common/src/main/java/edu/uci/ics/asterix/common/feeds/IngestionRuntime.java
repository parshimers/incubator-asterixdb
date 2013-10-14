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

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FrameReader;

public class IngestionRuntime implements ISubscribableRuntime {

    private final FeedIngestionId ingestionId;
    private IAdapterRuntimeManager adapterRuntimeManager;
    private final DistributeFeedFrameWriter feedWriter;

    public IngestionRuntime(FeedId feedId, int partition, IAdapterRuntimeManager adaptorRuntimeManager,
            DistributeFeedFrameWriter feedWriter) {
        this.ingestionId = new FeedIngestionId(feedId, partition);
        this.adapterRuntimeManager = adaptorRuntimeManager;
        this.feedWriter = feedWriter;
    }

    public void subscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        FrameReader reader = feedWriter.subscribeFeed(collectionRuntime.getFrameWriter());
        collectionRuntime.setFrameReader(reader);
        if (feedWriter.getDistributionMode().equals(DistributeFeedFrameWriter.DistributionMode.SINGLE)) {
            adapterRuntimeManager.start();
        }
    }

    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        feedWriter.unsubscribeFeed(collectionRuntime.getFrameWriter());
        if (feedWriter.getDistributionMode().equals(DistributeFeedFrameWriter.DistributionMode.INACTIVE)) {
            adapterRuntimeManager.stop();
        }
    }

    public void endOfFeed() {
        feedWriter.notifyEndOfFeed();
    }

    public IAdapterRuntimeManager getAdapterRuntimeManager() {
        return adapterRuntimeManager;
    }

    public FeedIngestionId getFeedIngestionId() {
        return ingestionId;
    }

    public void setAdapterRuntimeManager(IAdapterRuntimeManager adapterRuntimeManager) {
        this.adapterRuntimeManager = adapterRuntimeManager;
    }

    public FeedIngestionId getIngestionId() {
        return ingestionId;
    }

    public DistributeFeedFrameWriter getFeedWriter() {
        return feedWriter;
    }

    @Override
    public FeedId getFeedId() {
        return ingestionId.getFeedId();
    }

    @Override
    public IFeedFrameWriter getFeedFrameWriter() {
        return feedWriter;
    }

}
