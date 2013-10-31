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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FrameReader;

/**
 * Represents the runtime associated with computation of function associated with a feed.
 */
public class ComputeRuntime extends FeedRuntime implements ISubscribableRuntime {

    private final DistributeFeedFrameWriter feedWriter;
    private final List<ISubscriberRuntime> subscribers;

    public ComputeRuntime(FeedConnectionId feedConnectionId, int partition, FeedRuntimeType feedRuntimeType,
            DistributeFeedFrameWriter feedWriter) {
        super(feedConnectionId, partition, feedRuntimeType);
        this.feedWriter = feedWriter;
        this.subscribers = new ArrayList<ISubscriberRuntime>();
    }

    @Override
    public void subscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        FrameReader reader = feedWriter.subscribeFeed(collectionRuntime.getFrameWriter());
        collectionRuntime.setFrameReader(reader);
        subscribers.add(collectionRuntime);
    }

    @Override
    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        feedWriter.unsubscribeFeed(collectionRuntime.getFrameWriter());
        subscribers.remove(collectionRuntime);
    }

    @Override
    public IFeedFrameWriter getFeedFrameWriter() {
        return feedWriter;
    }

    public List<ISubscriberRuntime> getSubscribers() {
        return subscribers;
    }

}
