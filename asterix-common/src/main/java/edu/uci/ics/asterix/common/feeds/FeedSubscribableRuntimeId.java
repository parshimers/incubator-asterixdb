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

import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class FeedSubscribableRuntimeId {

    private final FeedId feedId;

    private final int partition;

    private final FeedRuntimeType feedRuntimeType;

    public FeedSubscribableRuntimeId(FeedId feedId, FeedRuntimeType feedRuntimeType, int partition) {
        this.feedId = feedId;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof FeedSubscribableRuntimeId)) {
            return false;
        }

        FeedSubscribableRuntimeId otherId = (FeedSubscribableRuntimeId) o;
        return this == o
                || (feedId.equals(otherId.feedId) && feedRuntimeType.equals(otherId.feedRuntimeType) && partition == otherId.partition);
    }

    @Override
    public int hashCode() {
        return (feedId.toString() + ":" + feedRuntimeType + "[" + partition + "]").hashCode();
    }

    @Override
    public String toString() {
        return feedId.toString() + ":" + feedRuntimeType + "[" + partition + "]";
    }

    public FeedId getFeedId() {
        return feedId;
    }

}
