package edu.uci.ics.asterix.common.feeds.api;

import java.util.List;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public interface IFeedJoint {

    public enum Type {
        /** Feed Joint is located on the ingestion path of a primary feed **/
        PRIMARY,

        /** Feed Joint is located on the ingestion path of a secondary feed **/
        SECONDARY
    }

    public enum State {
        /** Initial state of a feed joint post creation. **/
        CREATED,

        /** State acquired post creation of Hyracks job and known physical locations of the joint **/
        INITIALIZED,

        /** State acquired post starting of Hyracks job at which point, data begins to flow through the joint **/
        ACTIVE
    }

    public enum Scope {

        /** Feed Joint is not open to subscription by secondary feeds **/
        PRIVATE,

        /** Fed Joint is open to subscription by secondary feeds **/
        PUBLIC
    }

    /**
     * @return the {@link State} associated with the FeedJoint
     */
    public State getState();

    /**
     * @return the {@link Scope} associated witht the FeedJoint
     */
    public Scope getScope();

    /**
     * @return the {@link Type} associated with the FeedJoint
     */
    public Type getType();

    /**
     * @return the list of subscribers {@link FeedSubscriber} that have
     *         subscribed to data flowing through the FeedJoint
     */
    public List<FeedSubscriber> getSubscribers();

    /**
     * @return the list of pending subscription request {@link FeedSubscriptionRequest} submitted for data flowing through the FeedJoint
     */
    public List<FeedSubscriptionRequest> getSubscriptionRequests();

    /**
     * @return the subscription location {@link SubscriptionLocation} associated with the FeedJoint
     */
    public SubscriptionLocation getSubscriptionLocation();

    /**
     * @return the unique {@link FeedJointKey} associated with the FeedJoint
     */
    public FeedJointKey getFeedJointKey();

    /**
     * Returns the feed subscriber {@link FeedSubscriber} corresponding to a given feed connection id.
     * 
     * @param feedConnectionId
     *            the unique id of a feed connection
     * @return an instance of feed subscriber {@link FeedSubscriber} corresponding to the feed connection id
     */
    public FeedSubscriber getSubscriber(FeedConnectionId feedConnectionId);

    public void setJobId(JobId jobId);

    public void setJobSpec(JobSpecification jobSpec);

    public void setState(State active);

    public JobId getJobId();

    public JobSpecification getJobSpec();

    public void setLocations(List<String> locations);

    /**
     * Remove the subscriber from the set of registered subscribers to the FeedJoint
     * 
     * @param subscriber
     *            the subscriber that needs to be removed
     */
    public void removeSubscriber(FeedSubscriber subscriber);

    public FeedId getOwnerFeedId();

    /**
     * Returns the list of (physical) locations where the FeedJoint is located
     * 
     * @return
     */
    public List<String> getLocations();

    /**
     * Add a feed subscriber to the set of registered subscribers
     * 
     * @param subscriber
     */
    public void addSubscriber(FeedSubscriber subscriber);

    /**
     * Add a feed subscription request {@link FeedSubscriptionRequest} for the FeedJoint
     * 
     * @param request
     */
    public void addSubscriptionRequest(FeedSubscriptionRequest request);

}
