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

import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.IFeedConnectionManager;
import edu.uci.ics.asterix.common.feeds.IFeedIngestionManager;
import edu.uci.ics.asterix.common.feeds.IFeedManager;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedManager implements IFeedManager {

    private static final Logger LOGGER = Logger.getLogger(FeedManager.class.getName());

    private IFeedIngestionManager feedIngestionManager;

    private IFeedConnectionManager feedConnectionManager;

    private final String nodeId;

    public FeedManager(String nodeId) {
        this.nodeId = nodeId;
        this.feedIngestionManager = new FeedIngestionManager(nodeId);
        this.feedConnectionManager = new FeedConnectionManager(nodeId);
    }

    public IFeedIngestionManager getFeedIngestionManager() {
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
