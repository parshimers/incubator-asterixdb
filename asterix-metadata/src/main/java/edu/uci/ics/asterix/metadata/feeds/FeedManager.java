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

import edu.uci.ics.asterix.common.feeds.IFeedConnectionManager;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.IFeedManager;
import edu.uci.ics.asterix.common.feeds.IFeedWorkManager;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedManager implements IFeedManager {

    private final IFeedSubscriptionManager feedIngestionManager;

    private final IFeedConnectionManager feedConnectionManager;

    private final String nodeId;

    public FeedManager(String nodeId) {
        this.nodeId = nodeId;
        this.feedIngestionManager = new FeedSubscriptionManager(nodeId);
        this.feedConnectionManager = new FeedConnectionManager(nodeId);
    }

    public IFeedSubscriptionManager getFeedSubscriptionManager() {
        return feedIngestionManager;
    }

    public IFeedConnectionManager getFeedConnectionManager() {
        return feedConnectionManager;
    }

    @Override
    public String toString() {
        return "FeedManager " + "[" + nodeId + "]";
    }
}
