package edu.uci.ics.asterix.common.feeds.api;

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.message.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.message.ScaleInReportMessage;
import edu.uci.ics.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;

public interface IFeedLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(FeedCongestionMessage message) throws JSONException, AsterixException;

    public void submitFeedRuntimeReport(JSONObject obj);

    public void submitScaleInPossibleReport(ScaleInReportMessage sm) throws AsterixException, Exception;

    public List<String> getNodes(int n);

    public void reportThrottlingEnabled(ThrottlingEnabledFeedMessage mesg) throws AsterixException, Exception;

}
