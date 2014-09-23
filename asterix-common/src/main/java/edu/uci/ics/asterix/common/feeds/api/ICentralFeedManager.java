package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface ICentralFeedManager {

    public void start() throws AsterixException;

    public void stop() throws AsterixException;

    public IFeedTrackingManager getFeedTrackingManager();

    public IFeedLoadManager getFeedLoadManager();
}
