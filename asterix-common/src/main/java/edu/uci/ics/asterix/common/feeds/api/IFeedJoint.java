package edu.uci.ics.asterix.common.feeds.api;

import java.util.List;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.SubscriptionLocation;

public interface IFeedJoint {

    public enum Type {
        /** Feed Joint is located on the ingestion path of a primary feed **/
        INTAKE,

        /** Feed Joint is located on the ingestion path of a secondary feed **/
        COMPUTE
    }

    public enum State {
        /** Initial state of a feed joint post creation. **/
        CREATED,

        /** State acquired post creation of Hyracks job and known physical locations of the joint **/
        INITIALIZED,

        /** State acquired post starting of Hyracks job at which point, data begins to flow through the joint **/
        ACTIVE
    }

    /**
     * @return the {@link State} associated with the FeedJoint
     */
    public State getState();

    /**
     * @return the {@link Type} associated with the FeedJoint
     */
    public Type getType();

    /**
     * @return the list of subscribers {@link FeedSubscriber} that have
     *         subscribed to data flowing through the FeedJoint
     */
    public List<FeedConnectionId> getSubscribers();

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
     * @return an instance of feedConnectionId {@link FeedConnectionId}
     */
    public FeedConnectionId getSubscriber(FeedConnectionId feedConnectionId);

    /**
     * @param active
     */
    public void setState(State active);

    /**
     * Remove the subscriber from the set of registered subscribers to the FeedJoint
     * 
     * @param connectionId
     *            the connectionId that needs to be removed
     */
    public void removeSubscriber(FeedConnectionId connectionId);

    public FeedId getOwnerFeedId();

    /**
     * Add a feed connectionId to the set of registered subscribers
     * 
     * @param connectionId
     */
    public void addSubscriber(FeedConnectionId connectionId);

    /**
     * Add a feed subscription request {@link FeedSubscriptionRequest} for the FeedJoint
     * 
     * @param request
     */
    public void addSubscriptionRequest(FeedSubscriptionRequest request);

    public FeedConnectionId getProvider();

}
