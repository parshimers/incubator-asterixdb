package org.apache.asterix.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.feeds.FeedConnectionId;
import org.apache.asterix.common.feeds.FeedId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedCollectInfo extends FeedInfo {
    public FeedId sourceFeedId;
    public FeedConnectionId feedConnectionId;
    public List<String> collectLocations = new ArrayList<String>();
    public List<String> computeLocations = new ArrayList<String>();
    public List<String> storageLocations = new ArrayList<String>();
    public Map<String, String> feedPolicy;
    public String superFeedManagerHost;
    public int superFeedManagerPort;
    public boolean fullyConnected;

    public FeedCollectInfo(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobSpecification jobSpec,
            JobId jobId, Map<String, String> feedPolicy) {
        super(jobSpec, jobId, FeedInfoType.COLLECT);
        this.sourceFeedId = sourceFeedId;
        this.feedConnectionId = feedConnectionId;
        this.feedPolicy = feedPolicy;
        this.fullyConnected = true;
    }

    @Override
    public String toString() {
        return FeedInfoType.COLLECT + "[" + feedConnectionId + "]";
    }
}
