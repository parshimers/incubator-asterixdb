package org.apache.asterix.common.feeds.api;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedIntakeInfo;

public interface IFeedLifecycleIntakeEventSubscriber extends IFeedLifecycleEventSubscriber {

    public void handleFeedEvent(FeedIntakeInfo iInfo, FeedLifecycleEvent event) throws AsterixException;

}
