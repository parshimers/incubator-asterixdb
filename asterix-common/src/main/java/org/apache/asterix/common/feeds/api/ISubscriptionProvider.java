package org.apache.asterix.common.feeds.api;

import org.apache.asterix.common.feeds.FeedId;

public interface ISubscriptionProvider {

    public void subscribeFeed(FeedId sourceFeedId, FeedId recipientFeedId);

    public void unsubscribeFeed(FeedId sourceFeedId, FeedId recipientFeedId);

}
