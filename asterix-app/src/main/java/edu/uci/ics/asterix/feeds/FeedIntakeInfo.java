package edu.uci.ics.asterix.feeds;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedIntakeInfo extends FeedInfo {
    public List<String> intakeLocations = new ArrayList<String>();
    public FeedId feedId;

    public FeedIntakeInfo(FeedId feedId, JobSpecification jobSpec, JobId jobId) {
        super(jobSpec, jobId, FeedInfoType.INTAKE);
        this.feedId = feedId;
    }

    @Override
    public String toString() {
        return FeedInfoType.INTAKE + "[" + feedId + "]";
    }

}