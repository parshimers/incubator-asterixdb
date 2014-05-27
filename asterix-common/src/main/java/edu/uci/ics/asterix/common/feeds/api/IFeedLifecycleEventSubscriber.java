package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectJobInfo;

public interface IFeedLifecycleEventSubscriber {

    public enum FeedLifecycleEvent {
        FEED_STARTED,
        FEED_ENDED,
        FEED_SOFT_FAILURE,
        FEED_HARD_FAILURE
    }

    public void handleFeedEvent(FeedConnectJobInfo cInfo, FeedLifecycleEvent event) throws AsterixException;

    void waitForFeedCompletion() throws InterruptedException;
}
