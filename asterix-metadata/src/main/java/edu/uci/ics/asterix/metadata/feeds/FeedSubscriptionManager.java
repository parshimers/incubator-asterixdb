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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;

public class FeedSubscriptionManager implements IFeedSubscriptionManager {

    private static Logger LOGGER = Logger.getLogger(FeedSubscriptionManager.class.getName());

    private final String nodeId;

    private final Map<FeedSubscribableRuntimeId, ISubscribableRuntime> ingestionRuntimes = new HashMap<FeedSubscribableRuntimeId, ISubscribableRuntime>();

    public FeedSubscriptionManager(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void registerFeedSubscribableRuntime(ISubscribableRuntime subscribableRuntime) {
        if (!ingestionRuntimes.containsKey(subscribableRuntime.getFeedSubscribableRuntimeId())) {
            ingestionRuntimes.put(subscribableRuntime.getFeedSubscribableRuntimeId(), subscribableRuntime);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed subscribable runtime " + subscribableRuntime);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed ingestion runtime " + subscribableRuntime + " already registered.");
            }
        }
    }

    @Override
    public ISubscribableRuntime getSubscribableRuntime(FeedSubscribableRuntimeId feedIngestionId) {
        return ingestionRuntimes.get(feedIngestionId);
    }

    @Override
    public void deregisterFeedSubscribableRuntime(FeedSubscribableRuntimeId ingestionId) {
        ingestionRuntimes.remove(ingestionId);
    }

    @Override
    public String toString() {
        return "IngestionManager [" + nodeId + "]";
    }

}
