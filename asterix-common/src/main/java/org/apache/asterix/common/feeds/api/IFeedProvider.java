package org.apache.asterix.common.feeds.api;

import org.apache.asterix.common.feeds.FeedId;

public interface IFeedProvider {

    public void subscribeFeed(FeedId sourceDeedId);
}
