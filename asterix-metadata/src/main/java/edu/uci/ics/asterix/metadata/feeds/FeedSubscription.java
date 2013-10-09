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

import edu.uci.ics.asterix.metadata.entities.Feed;

public class FeedSubscription {

    /**
     * The source of data for the feed. This is null in the case of a primary feed
     */
    private final Feed sourceFeed;

    /**
     * The feed handle representing the feed that is subscribing for data.
     */
    private final Feed feed;

    /**
     * The status associated with the subscription.
     */
    private SubscriptionStatus subscriptionStatus;

    public FeedSubscription(Feed feed, Feed sourceFeed) {
        this.feed = feed;
        this.sourceFeed = sourceFeed;
        this.subscriptionStatus = SubscriptionStatus.INITIALIZED;
    }

    public enum SubscriptionStatus {
        INITIALIZED, // initial state upon creating a subscription request
        ACTIVE, // subscribing feed receives data 
        INACTIVE // subscribing feed does not receive data 
    }

    public SubscriptionStatus getSubscriptionStatus() {
        return subscriptionStatus;
    }

    public void setSubscriptionStatus(SubscriptionStatus subscriptionStatus) {
        this.subscriptionStatus = subscriptionStatus;
    }

    public Feed getSourceFeed() {
        return sourceFeed;
    }

    public Feed getFeed() {
        return feed;
    }

}
