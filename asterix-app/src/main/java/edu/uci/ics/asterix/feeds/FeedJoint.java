/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.asterix.common.feeds.IFeedJoint;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedJoint implements IFeedJoint {

    private static final Logger LOGGER = Logger.getLogger(FeedJoint.class.getName());

    /** A unique key associated with the feed point **/
    private final FeedJointKey key;

    /** The locations at which data from the FeedJoint can be found **/
    private List<String> locations;

    /** The state associated with the FeedJoint **/
    private State state;

    /** A list of subscribers that receive data from this FeedJoint **/
    private final List<FeedSubscriber> subscribers;

    /** The jobId associated with the job that produces the FeedJoint's data **/
    private JobId jobId;

    /** The job specification of the job that produces the FeedJsoint's data **/
    private JobSpecification jobSpec;

    /** The feedId on which the feedPoint resides **/
    private final FeedId ownerFeedId;

    /** A list of feed subscription requests submitted for subscribing to the FeedPoint's data **/
    private final List<FeedSubscriptionRequest> subscriptionRequests;

    private final SubscriptionLocation subscriptionLocation;

    private final Type type;

    private Scope scope;

    public FeedJoint(FeedJointKey key, FeedId ownerFeedId, SubscriptionLocation subscriptionLocation, Type type,
            Scope scope) {
        this.key = key;
        this.ownerFeedId = ownerFeedId;
        this.type = type;
        this.scope = scope;
        this.subscribers = new ArrayList<FeedSubscriber>();
        this.state = State.CREATED;
        this.subscriptionLocation = subscriptionLocation;
        this.subscriptionRequests = new ArrayList<FeedSubscriptionRequest>();
    }

    @Override
    public String toString() {
        return key.toString() + " [" + subscriptionLocation + "]" + "[" + state + "]";
    }

    public void addSubscriber(FeedSubscriber feedSubscriber) {
        subscribers.add(feedSubscriber);
    }

    public void removeSubscriber(FeedSubscriber feedSubscriber) {
        subscribers.remove(feedSubscriber);
    }

    public synchronized void addSubscriptionRequest(FeedSubscriptionRequest request) {
        subscriptionRequests.add(request);
        if (state.equals(State.ACTIVE)) {
            handlePendingSubscriptionRequest();
        }
    }

    public List<FeedSubscriber> getSubscribers() {
        return subscribers;
    }

    public FeedJointKey getKey() {
        return key;
    }

    public List<String> getLocations() {
        return locations;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        if (this.state.equals(state)) {
            return;
        }
        this.state = state;
        if (this.state.equals(State.ACTIVE)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Feed joint " + this + " is now " + State.ACTIVE);
            }
            handlePendingSubscriptionRequest();
        }
    }

    @Override
    public FeedJointKey getFeedJointKey() {
        return key;
    }

    private void handlePendingSubscriptionRequest() {
        for (FeedSubscriptionRequest subscriptionRequest : subscriptionRequests) {
            FeedConnectionId connectionId = new FeedConnectionId(subscriptionRequest.getSubscribingFeedId(),
                    subscriptionRequest.getTargetDataset());
            try {
                FeedLifecycleListener.INSTANCE.submitFeedSubscriptionRequest(this, subscriptionRequest);
                FeedSubscriber subscriber = new FeedSubscriber(this.getFeedJointKey(), connectionId,
                        subscriptionRequest.getPolicy(), subscriptionRequest.getPolicyParameters(),
                        FeedSubscriber.Status.CREATED);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Submitted feed subscrtion request " + subscriptionRequest + " at feed joint " + this);
                }
                subscribers.add(subscriber);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unsuccessful attempt at submitting subscription request " + subscriptionRequest
                            + " at feed point " + this + ". Message " + e.getMessage());
                }
                e.printStackTrace();
            }
        }
        subscriptionRequests.clear();
    }

    public void setLocations(List<String> locations) {
        this.locations = locations;
    }

    public boolean hasSubscriber(FeedConnectionId feedConnectionId) {
        return getSubscriber(feedConnectionId) != null;
    }

    public FeedSubscriber getSubscriber(FeedConnectionId feedConnectionId) {
        for (FeedSubscriber subscriber : subscribers) {
            if (subscriber.getFeedConnectionId().equals(feedConnectionId)) {
                return subscriber;
            }
        }
        return null;
    }

    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public JobSpecification getJobSpec() {
        return jobSpec;
    }

    public void setJobSpec(JobSpecification jobSpec) {
        this.jobSpec = jobSpec;
    }

    public FeedId getOwnerFeedId() {
        return ownerFeedId;
    }

    public List<FeedSubscriptionRequest> getSubscriptionRequests() {
        return subscriptionRequests;
    }

    public SubscriptionLocation getSubscriptionLocation() {
        return subscriptionLocation;
    }

    public Type getType() {
        return type;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

}
