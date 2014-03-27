package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.feeds.IFeedLifecycleEventSubscriber;

public class FeedLifecycleEventSubscriber implements IFeedLifecycleEventSubscriber {

    private boolean isFeedActive = true;

    @Override
    public void handleFeedEvent(FeedLifecycleEvent event) {
        synchronized (this) {
            switch (event) {
                case FEED_ENDED:
                    isFeedActive = false;
                    this.notifyAll();
                    break;
            }
        }
    }

    @Override
    public void waitForFeedCompletion() throws InterruptedException {
        synchronized (this) {
            while (isFeedActive) {
                this.wait();
            }
        }
    }
}
