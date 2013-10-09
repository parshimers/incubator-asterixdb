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

public class FeedIngestionId {

    private final FeedId feedId;

    private final int partition;

    public FeedIngestionId(FeedId feedId, int partition) {
        this.feedId = feedId;
        this.partition = partition;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public int getPartition() {
        return partition;
    }

}
