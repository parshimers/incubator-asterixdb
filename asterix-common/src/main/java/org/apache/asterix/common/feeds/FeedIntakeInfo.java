package org.apache.asterix.common.feeds;

import java.util.List;

import org.apache.asterix.common.feeds.api.IFeedJoint;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class FeedIntakeInfo extends FeedJobInfo {

    private final FeedId feedId;
    private final IFeedJoint intakeFeedJoint;
    private final JobSpecification spec;
    private List<String> intakeLocation;

    public FeedIntakeInfo(JobId jobId, FeedJobState state, JobType jobType, FeedId feedId, IFeedJoint intakeFeedJoint,
            JobSpecification spec) {
        super(jobId, state, FeedJobInfo.JobType.INTAKE, spec);
        this.feedId = feedId;
        this.intakeFeedJoint = intakeFeedJoint;
        this.spec = spec;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public IFeedJoint getIntakeFeedJoint() {
        return intakeFeedJoint;
    }

    public JobSpecification getSpec() {
        return spec;
    }

    public List<String> getIntakeLocation() {
        return intakeLocation;
    }

    public void setIntakeLocation(List<String> intakeLocation) {
        this.intakeLocation = intakeLocation;
    }

}
