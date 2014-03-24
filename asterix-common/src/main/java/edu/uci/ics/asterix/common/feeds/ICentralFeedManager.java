package edu.uci.ics.asterix.common.feeds;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface ICentralFeedManager {

    public void start() throws AsterixException;

    public void stop() throws AsterixException;


}
