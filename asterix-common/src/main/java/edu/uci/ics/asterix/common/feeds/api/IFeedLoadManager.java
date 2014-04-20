package edu.uci.ics.asterix.common.feeds.api;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public interface IFeedLoadManager {

    public void submitNodeLoadReport(NodeLoadReport report);

    public void reportCongestion(JSONObject report) throws JSONException;

    public JobSpecification reEvaluateJob(JobSpecification jobSpec);

    public void submitFeedRuntimeReport(JSONObject obj);
}
