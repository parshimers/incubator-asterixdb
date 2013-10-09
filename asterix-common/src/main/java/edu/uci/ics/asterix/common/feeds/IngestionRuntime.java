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

public class IngestionRuntime {

    private final FeedIngestionId ingestionId;
    private IAdapterRuntimeManager adapterRuntimeManager;
    private final DistributeFeedFrameWriter feedWriter;

    public IngestionRuntime(FeedId feedId, int partition, IAdapterRuntimeManager adaptorRuntimeManager,
            DistributeFeedFrameWriter feedWriter) {
        this.ingestionId = new FeedIngestionId(feedId, partition);
        this.adapterRuntimeManager = adaptorRuntimeManager;
        this.feedWriter = feedWriter;
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

}
