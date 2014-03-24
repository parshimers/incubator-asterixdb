package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IFeedMetadataManager {

    public void logTuple(FeedConnectionId feedConnectionId, String tuple, String message, IFeedManager feedManager) throws AsterixException;

}
