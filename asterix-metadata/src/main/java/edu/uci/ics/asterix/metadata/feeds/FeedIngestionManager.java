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

import edu.uci.ics.asterix.common.feeds.FeedIngestionId;
import edu.uci.ics.asterix.common.feeds.IFeedIngestionManager;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;

public class FeedIngestionManager implements IFeedIngestionManager {

    private static Logger LOGGER = Logger.getLogger(FeedIngestionManager.class.getName());

    private final String nodeId;

    private final Map<FeedIngestionId, IngestionRuntime> ingestionRuntimes = new HashMap<FeedIngestionId, IngestionRuntime>();

    public FeedIngestionManager(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void registerFeedIngestionRuntime(IngestionRuntime ingestionRuntime) {
        if (!ingestionRuntimes.containsKey(ingestionRuntime.getFeedIngestionId())) {
            ingestionRuntimes.put(ingestionRuntime.getFeedIngestionId(), ingestionRuntime);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed ingestion runtime " + ingestionRuntime);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed ingestion runtime " + ingestionRuntime + " already registered.");
            }
        }
    }

    @Override
    public IngestionRuntime getIngestionRuntime(FeedIngestionId feedIngestionId) {
        return ingestionRuntimes.get(feedIngestionId);
    }

    @Override
    public void deregisterFeedIngestionRuntime(FeedIngestionId ingestionId) {
        ingestionRuntimes.remove(ingestionId);
    }

    @Override
    public String toString() {
        return "IngestionManager [" + nodeId + "]";
    }

}
