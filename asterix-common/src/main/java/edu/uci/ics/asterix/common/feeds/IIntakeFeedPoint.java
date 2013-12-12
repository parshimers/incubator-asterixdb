package edu.uci.ics.asterix.common.feeds;

public interface IIntakeFeedPoint extends IFeedPoint {

    public void addSubscriptionRequest(FeedSubscriptionRequest request);
}
