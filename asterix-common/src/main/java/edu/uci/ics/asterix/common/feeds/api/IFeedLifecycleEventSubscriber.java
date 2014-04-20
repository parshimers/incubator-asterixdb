package edu.uci.ics.asterix.common.feeds.api;

public interface IFeedLifecycleEventSubscriber {

    public enum FeedLifecycleEvent {
        FEED_STARTED,
        FEED_ENDED,
        FEED_SOFT_FAILURE,
        FEED_HARD_FAILURE
    }

    public void handleFeedEvent(FeedLifecycleEvent event);
    
    void waitForFeedCompletion() throws InterruptedException;
}
