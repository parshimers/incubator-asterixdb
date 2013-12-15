package edu.uci.ics.asterix.common.feeds;

import java.util.List;

import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public interface IFeedJoint {

    public enum Type {
        PRIMARY,
        SECONDARY
    }

    public enum State {
        CREATED,
        INITIALIZED,
        ACTIVE // data is flowing through the feed point
    }

    public enum Scope {
        PRIVATE,
        PUBLIC
    }

    public State getState();

    public Scope getScope();

    public Type getType();

    public List<FeedSubscriber> getSubscribers();

    public List<FeedSubscriptionRequest> getSubscriptionRequests();

    public SubscriptionLocation getSubscriptionLocation();

    public FeedJointKey getFeedJointKey();

    public FeedSubscriber getSubscriber(FeedConnectionId feedConnectionId);

    public void setJobId(JobId jobId);

    public void setJobSpec(JobSpecification jobSpec);

    public void setState(State active);

    public JobId getJobId();

    public JobSpecification getJobSpec();

    public void setLocations(List<String> locations);

    public void removeSubscriber(FeedSubscriber subscriber);

    public FeedId getOwnerFeedId();

    public List<String> getLocations();

    public void addSubscriber(FeedSubscriber subscriber);

    public void addSubscriptionRequest(FeedSubscriptionRequest request);

}
