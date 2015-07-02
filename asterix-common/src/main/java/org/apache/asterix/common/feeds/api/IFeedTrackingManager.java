package org.apache.asterix.common.feeds.api;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedTupleCommitAckMessage;

public interface IFeedTrackingManager {

    public void submitAckReport(FeedTupleCommitAckMessage ackMessage);

    public void disableAcking(FeedConnectionId connectionId);
}
