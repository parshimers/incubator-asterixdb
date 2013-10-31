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

import java.util.Map;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.FrameReader;

/**
 * Represents the feed runtime that collects feed tuples from another feed.
 * In case of a primary feed, the collect runtime collects tuples from the feed
 * ingestion job. For a secondary feed, tuples are collected from the ingestion/compute
 * runtime associated with the source feed.
 */
public class CollectionRuntime extends FeedRuntime implements ISubscriberRuntime {

    private ISubscribableRuntime sourceRuntime;
    private FrameReader reader;
    private IFeedFrameWriter feedFrameWriter;
    private Map<String, String> feedPolicy;

    public CollectionRuntime(FeedConnectionId feedId, int partition, IFeedFrameWriter feedFrameWriter,
            ISubscribableRuntime sourceRuntime, Map<String, String> feedPolicy) {
        super(feedId, partition, FeedRuntimeType.COLLECT);
        this.sourceRuntime = sourceRuntime;
        this.feedFrameWriter = feedFrameWriter;
        this.feedPolicy = feedPolicy;
    }

    public ISubscribableRuntime getSubscribableRuntime() {
        return sourceRuntime;
    }

    public void setIngestionRuntime(ISubscribableRuntime sourceRuntime) {
        this.sourceRuntime = sourceRuntime;
    }

    public void setFrameReader(FrameReader reader) {
        this.reader = reader;
    }

    public FrameReader getReader() {
        return reader;
    }

    public IFeedFrameWriter getFrameWriter() {
        return feedFrameWriter;
    }

    public void setFrameWriter(IFeedFrameWriter frameWriter) {
        this.feedFrameWriter = frameWriter;
    }

    public void waitTillCollectionOver() throws InterruptedException {
        synchronized (reader) {
            while (!reader.getState().equals(FrameReader.State.FINISHED)) {
                reader.wait();
            }
        }
    }

    @Override
    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

}
