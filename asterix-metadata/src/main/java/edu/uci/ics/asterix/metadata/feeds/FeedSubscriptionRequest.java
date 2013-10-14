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

import java.util.List;

import edu.uci.ics.asterix.metadata.entities.Feed;

/**
 * A request for subscribing to a feed.
 */
public class FeedSubscriptionRequest {

    /** The source of data for the feed. This is null in the case of a primary feed */
    private final Feed sourceFeed;

    /** The feed handle representing the feed that is subscribing for data. */
    private final Feed feed;

    /** An ordered list of functions that need to be applied to receiving tuple to produce the feed. **/
    private final List<String> appliedFunctions;

    /** The status associated with the subscription. */
    private SubscriptionStatus subscriptionStatus;

    /** The policy associated with a feed connection. **/
    private final String policy;

    /** The target dataset that would receive the feed. **/
    private final String targetDataset;

    /** Represents the location in the source feed pipeline from where feed tuples are received. **/
    private final SubscriptionLocation subscriptionLocation;

    public enum SubscriptionLocation {
        SOURCE_FEED_INTAKE,
        SOURCE_FEED_COMPUTE
    }

    public FeedSubscriptionRequest(Feed feed, Feed sourceFeed, SubscriptionLocation subscriptionLocation,
            List<String> appliedFunctions, String targetDataset, String policy) {
        this.feed = feed;
        this.sourceFeed = sourceFeed;
        this.subscriptionLocation = subscriptionLocation;
        this.appliedFunctions = appliedFunctions;
        this.targetDataset = targetDataset;
        this.policy = policy;
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

    public List<String> getAssociatedFunctions() {
        return appliedFunctions;
    }

    public String getPolicy() {
        return policy;
    }

    public String getTargetDataset() {
        return targetDataset;
    }

    public SubscriptionLocation getSubscriptionLocation() {
        return subscriptionLocation;
    }

    @Override
    public String toString() {
        return sourceFeed.getFeedId() + " --> " + feed.getFeedId();
    }

}
