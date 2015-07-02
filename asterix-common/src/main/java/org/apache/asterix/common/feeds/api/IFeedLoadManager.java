package org.apache.asterix.common.feeds.api;

import java.util.Collection;
import java.util.List;

import org.json.JSONException;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.FeedActivity;
import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.NodeLoadReport;
import org.apache.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import org.apache.asterix.common.feeds.message.FeedCongestionMessage;
import org.apache.asterix.common.feeds.message.FeedReportMessage;
import org.apache.asterix.common.feeds.message.ScaleInReportMessage;
import org.apache.asterix.common.feeds.message.ThrottlingEnabledFeedMessage;

public interface IFeedLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(FeedCongestionMessage message) throws JSONException, AsterixException;

    public void submitFeedRuntimeReport(FeedReportMessage message);

    public void submitScaleInPossibleReport(ScaleInReportMessage sm) throws AsterixException, Exception;

    public List<String> getNodes(int required);

    public void reportThrottlingEnabled(ThrottlingEnabledFeedMessage mesg) throws AsterixException, Exception;

    int getOutflowRate(FeedConnectionId connectionId, FeedRuntimeType runtimeType);

    void reportFeedActivity(FeedConnectionId connectionId, FeedActivity activity);

    void removeFeedActivity(FeedConnectionId connectionId);
    
    public FeedActivity getFeedActivity(FeedConnectionId connectionId);

    public Collection<FeedActivity> getFeedActivities();

}
