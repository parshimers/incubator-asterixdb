/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * Copyright 2009-2013 by The Regents of the University of California
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import java.util.List;

public class FeedConnectionInfo {

    private final FeedConnectionId feedConnectionId;
    private final List<String> collectLocations;
    private final List<String> computeLocations;
    private final List<String> storageLocations;

    public FeedConnectionInfo(FeedConnectionId feedConnectionId, List<String> collectLocations,
            List<String> computeLocations, List<String> storageLocations) {
        this.feedConnectionId = feedConnectionId;
        this.collectLocations = collectLocations;
        this.computeLocations = computeLocations;
        this.storageLocations = storageLocations;
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public List<String> getCollectLocations() {
        return collectLocations;
    }

    public List<String> getComputeLocations() {
        return computeLocations;
    }

    public List<String> getStorageLocations() {
        return storageLocations;
    }

}
