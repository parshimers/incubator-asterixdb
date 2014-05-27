package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.feeds.FeedTupleCommitAckMessage;

public interface IFeedTrackingManager {

    public void submitAckReport(FeedTupleCommitAckMessage ackMessage);
}
