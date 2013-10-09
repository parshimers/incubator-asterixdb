package edu.uci.ics.asterix.common.feeds;

public interface ISubscriptionProvider {

    public void subscribeFeed(FeedId sourceFeedId, FeedId recipientFeedId);

    public void unsubscribeFeed(FeedId sourceFeedId, FeedId recipientFeedId);

}
