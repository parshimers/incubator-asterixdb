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
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class SubscribableRuntime implements ISubscribableRuntime {

    protected static final Logger LOGGER = Logger.getLogger(SubscribableRuntime.class.getName());

    protected final FeedSubscribableRuntimeId subscribableRuntimeId;

    protected final FeedRuntimeType runtimeType;

    protected final DistributeFeedFrameWriter feedWriter;

    protected final List<ISubscriberRuntime> subscribers;

    protected RecordDescriptor recordDescriptor;

    public SubscribableRuntime(FeedSubscribableRuntimeId runtimeId, DistributeFeedFrameWriter feedWriter,
            FeedRuntimeType runtimeType, RecordDescriptor recordDescriptor) {
        this.subscribableRuntimeId = runtimeId;
        this.feedWriter = feedWriter;
        this.runtimeType = runtimeType;
        this.recordDescriptor = recordDescriptor;
        this.subscribers = new ArrayList<ISubscriberRuntime>();
    }

    @Override
    public FeedId getFeedId() {
        return subscribableRuntimeId.getFeedId();
    }

    @Override
    public String toString() {
        return "SubscribableRuntime" + " [" + subscribableRuntimeId + "]";
    }

    @Override
    public void subscribeFeed(FeedPolicyAccessor fpa, CollectionRuntime collectionRuntime) throws Exception {
        FeedFrameCollector collector = feedWriter.subscribeFeed(
                new FeedPolicyAccessor(collectionRuntime.getFeedPolicy()), collectionRuntime.getFeedFrameWriter());
        collectionRuntime.setFrameCollector(collector);
        subscribers.add(collectionRuntime);
    }

    @Override
    public void unsubscribeFeed(CollectionRuntime collectionRuntime) throws Exception {
        feedWriter.unsubscribeFeed(collectionRuntime.getFeedFrameWriter());
        subscribers.remove(collectionRuntime);
    }

    @Override
    public List<ISubscriberRuntime> getSubscribers() {
        return subscribers;
    }

    @Override
    public DistributeFeedFrameWriter getFeedFrameWriter() {
        return feedWriter;
    }

    @Override
    public FeedSubscribableRuntimeId getFeedSubscribableRuntimeId() {
        return subscribableRuntimeId;
    }

    @Override
    public FeedRuntimeType getFeedRuntimeType() {
        return runtimeType;
    }

    @Override
    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

}
