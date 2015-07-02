package org.apache.asterix.common.feeds.api;

import java.io.IOException;

import org.apache.asterix.common.exceptions.AsterixException;

public interface ICentralFeedManager {

    public void start() throws AsterixException;

    public void stop() throws AsterixException, IOException;

    public IFeedTrackingManager getFeedTrackingManager();

    public IFeedLoadManager getFeedLoadManager();
}
