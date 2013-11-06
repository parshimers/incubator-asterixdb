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

public class FeedSubscribableRuntimeId {

    private final FeedId feedId;

    private final int partition;

    public FeedSubscribableRuntimeId(FeedId feedId, int partition) {
        this.feedId = feedId;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedSubscribableRuntimeId)) {
            return true;
        }

        FeedSubscribableRuntimeId otherId = (FeedSubscribableRuntimeId) o;
        return feedId.equals(otherId.feedId) && partition == otherId.partition;
    }

    @Override
    public int hashCode() {
        return (feedId.toString() + "[" + partition + "]").hashCode();
    }

    @Override
    public String toString() {
        return feedId.toString() + "[" + partition + "]";
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public int getPartition() {
        return partition;
    }

}
