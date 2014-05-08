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
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.SubscriptionLocation;

public class FeedJoint implements IFeedJoint {

    private static final Logger LOGGER = Logger.getLogger(FeedJoint.class.getName());

    /** A unique key associated with the feed point **/
    private final FeedJointKey key;

    /** The state associated with the FeedJoint **/
    private State state;

    /** A list of subscribers that receive data from this FeedJoint **/
    private final List<FeedConnectionId> subscribers;

    /** The feedId on which the feedPoint resides **/
    private final FeedId ownerFeedId;

    /** A list of feed subscription requests submitted for subscribing to the FeedPoint's data **/
    private final List<FeedSubscriptionRequest> subscriptionRequests;

    private final SubscriptionLocation subscriptionLocation;

    private final Type type;

    private FeedConnectionId provider;

    public FeedJoint(FeedJointKey key, FeedId ownerFeedId, SubscriptionLocation subscriptionLocation, Type type,
            FeedConnectionId provider) {
        this.key = key;
        this.ownerFeedId = ownerFeedId;
        this.type = type;
        this.subscribers = new ArrayList<FeedConnectionId>();
        this.state = State.CREATED;
        this.subscriptionLocation = subscriptionLocation;
        this.subscriptionRequests = new ArrayList<FeedSubscriptionRequest>();
        this.provider = provider;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public void addSubscriber(FeedConnectionId connectionId) {
        subscribers.add(connectionId);
    }

    public void removeSubscriber(FeedConnectionId connectionId) {
        subscribers.remove(connectionId);
    }

    public synchronized void addSubscriptionRequest(FeedSubscriptionRequest request) {
        subscriptionRequests.add(request);
        if (state.equals(State.ACTIVE)) {
            handlePendingSubscriptionRequest();
        }
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

    private void handlePendingSubscriptionRequest() {
        for (FeedSubscriptionRequest subscriptionRequest : subscriptionRequests) {
            FeedConnectionId connectionId = new FeedConnectionId(subscriptionRequest.getSubscribingFeedId(),
                    subscriptionRequest.getTargetDataset());
            try {
                FeedLifecycleListener.INSTANCE.submitFeedSubscriptionRequest(this, subscriptionRequest);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Submitted feed subscription request " + subscriptionRequest + " at feed joint " + this);
                }
                addSubscriber(connectionId);
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

    public FeedConnectionId getSubscriber(FeedConnectionId connectionId) {
        for (FeedConnectionId cid : subscribers) {
            if (cid.equals(connectionId)) {
                return cid;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return key.toString() + " [" + subscriptionLocation + "]" + "[" + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (o == this) {
            return true;
        }
        if (!(o instanceof FeedJoint)) {
            return false;
        }
        return ((FeedJoint) o).getFeedJointKey().equals(this.key);
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

    @Override
    public FeedConnectionId getProvider() {
        return provider;
    }

    public List<FeedConnectionId> getSubscribers() {
        return subscribers;
    }

    public FeedJointKey getKey() {
        return key;
    }

    public synchronized State getState() {
        return state;
    }

    @Override
    public FeedJointKey getFeedJointKey() {
        return key;
    }

}
