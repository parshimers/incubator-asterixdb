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

import edu.uci.ics.asterix.common.feeds.FeedOperatorInputSideHandler.Mode;
import edu.uci.ics.asterix.common.feeds.api.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.api.ISubscriberRuntime;

/**
 * Represents the feed runtime that collects feed tuples from another feed.
 * In case of a primary feed, the collect runtime collects tuples from the feed
 * intake job. For a secondary feed, tuples are collected from the intake/compute
 * runtime associated with the source feed.
 */
public class CollectionRuntime extends BasicFeedRuntime implements ISubscriberRuntime {

    private final ISubscribableRuntime sourceRuntime;
    private final Map<String, String> feedPolicy;
    private FeedFrameCollector frameCollector;
    private Mode mode;

    public CollectionRuntime(FeedConnectionId feedId, int partition, FeedOperatorInputSideHandler inputSideHandler,
            ISubscribableRuntime sourceRuntime, Map<String, String> feedPolicy, FeedRuntimeType runtimeType) {
        super(feedId, partition, inputSideHandler, runtimeType, FeedRuntimeId.DEFAULT_OPERAND_ID);
        this.sourceRuntime = sourceRuntime;
        this.feedPolicy = feedPolicy;
        this.mode = Mode.PROCESS;
    }

    public ISubscribableRuntime getSourceRuntime() {
        return sourceRuntime;
    }

    public void setFrameCollector(FeedFrameCollector frameCollector) {
        this.frameCollector = frameCollector;
    }

    public FeedFrameCollector getFrameCollector() {
        return frameCollector;
    }

    public void waitTillCollectionOver() throws InterruptedException {
        if (!frameCollector.getState().equals(FeedFrameCollector.State.FINISHED)) {
            synchronized (frameCollector) {
                while (!frameCollector.getState().equals(FeedFrameCollector.State.FINISHED)) {
                    frameCollector.wait();
                }
            }
        }
    }

    public void setMode(Mode mode) {
        FeedOperatorInputSideHandler inputSideHandler = (FeedOperatorInputSideHandler) getFeedFrameWriter();
        inputSideHandler.setMode(mode);
    }

    @Override
    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

}
