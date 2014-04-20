package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;

public interface IFeedMetadataManager {

    public void logTuple(FeedConnectionId feedConnectionId, String tuple, String message, IFeedManager feedManager) throws AsterixException;

}
