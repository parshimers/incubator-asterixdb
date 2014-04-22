package edu.uci.ics.asterix.common.feeds.api;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;

public interface IFeedLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(FeedCongestionMessage message) throws JSONException, AsterixException;

    public void submitFeedRuntimeReport(JSONObject obj);
}
