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
package edu.uci.ics.asterix.file;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.bootstrap.FeedLifecycleListener;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;

/**
 * Provides helper method(s) for creating JobSpec for operations on a feed.
 */
public class FeedOperations {

    private static Logger LOGGER = Logger.getLogger(FeedOperations.class.getName());

    /**
     * Builds the job spec for ingesting a (primary) feed from its external source via the feed adaptor.
     * 
     * @param primaryFeed
     * @param metadataProvider
     * @return
     * @throws Exception
     */
    public static JobSpecification buildFeedIntakeJobSpec(PrimaryFeed primaryFeed, AqlMetadataProvider metadataProvider)
            throws Exception {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedIngestor;
        AlgebricksPartitionConstraint ingesterPc;

        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider.buildFeedIntakeRuntime(spec,
                    primaryFeed);
            feedIngestor = p.first;
            ingesterPc = p.second;
        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedIngestor, ingesterPc);

        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, ingesterPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedIngestor, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return spec;

    }

    public static JobSpecification buildDiscontinueFeedSourceSpec(AqlMetadataProvider metadataProvider, FeedId feedId)
            throws AsterixException, AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger = null;
        AlgebricksPartitionConstraint messengerPc = null;

        String[] locations = FeedLifecycleListener.INSTANCE.getIntakeLocations(feedId);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider
                .buildDiscontinueFeedMessengerRuntime(spec, feedId, locations);

        feedMessenger = p.first;
        messengerPc = p.second;

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
        spec.addRoot(nullSink);

        return spec;
    }

    /**
     * Builds the job spec for sending message to an active feed to disconnect it from the
     * its source.
     * 
     * @param dataverseName
     * @param feedName
     * @param datasetName
     * @param metadataProvider
     * @param feedActivity
     * @return
     * @throws AsterixException
     * @throws AlgebricksException
     */
    public static Triple<JobSpecification, Boolean, Boolean> buildDisconnectFeedJobSpec(
            AqlMetadataProvider metadataProvider, FeedConnectionId feedConnectionId) throws AsterixException,
            AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;
        boolean completeDisconnection;
        FeedId sourceFeedId;
        boolean needToDiscontinueSourceFeed = false;

        try {
            Pair<SubscriptionLocation, List<FeedConnectionId>> subscriptions = FeedLifecycleListener.INSTANCE
                    .getFeedSubscriptions(feedConnectionId.getFeedId());
            boolean dependentSubscribers = (subscriptions != null && subscriptions.second.size() > 0);
            String[] locations = null;
            FeedRuntimeType subscribableRuntmeType = null;
            if (!dependentSubscribers) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Feed connection " + feedConnectionId
                            + " has no subscribers to the connection. [COMPLETE DISCONNECTION]");
                }

                completeDisconnection = true;
                // find where this feed is receiving its data from and unsubscribe from the joint. 
                Pair<FeedId, SubscriptionLocation> sourceFeedInfo = FeedLifecycleListener.INSTANCE
                        .getSourceFeedInfo(feedConnectionId);
                sourceFeedId = sourceFeedInfo.first;

                switch (sourceFeedInfo.second) {
                    case SOURCE_FEED_INTAKE:
                        //case for a primary feed that has no subscribers
                        locations = FeedLifecycleListener.INSTANCE.getIntakeLocations(sourceFeedInfo.first);
                        subscribableRuntmeType = FeedRuntimeType.COLLECT;
                        break;
                    case SOURCE_FEED_COMPUTE:
                        // case of a secondary feed that has no subscribers
                        locations = FeedLifecycleListener.INSTANCE.getComputeLocations(sourceFeedInfo.first);
                        subscribableRuntmeType = FeedRuntimeType.COMPUTE;

                        Pair<SubscriptionLocation, List<FeedConnectionId>> subscriptionsOfSource = FeedLifecycleListener.INSTANCE
                                .getFeedSubscriptions(sourceFeedInfo.first);
                        if (subscriptionsOfSource.second.size() == 1) {
                            // other subscribers are absent, so parent feed should discontinue
                            needToDiscontinueSourceFeed = true;
                        }
                        break;
                }
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Feed disconnect message would be sent to " + StringUtils.join(locations, ','));
                }

            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Feed connection " + feedConnectionId
                            + " has no subscribers to the connection. [PARTIAL DISCONNECTION]");
                }
                sourceFeedId = feedConnectionId.getFeedId();
                completeDisconnection = false;
                // find the location where the feed is offering data to its subscribers and unsubscribe itself 
                // at that joint
                SubscriptionLocation subscriptionLocation = subscriptions.first;
                switch (subscriptionLocation) {
                    case SOURCE_FEED_INTAKE:
                        throw new IllegalStateException("Data hand-off at " + subscriptionLocation
                                + " should not be counted as subscription");

                    case SOURCE_FEED_COMPUTE:
                        locations = FeedLifecycleListener.INSTANCE.getComputeLocations(feedConnectionId.getFeedId());
                        subscribableRuntmeType = FeedRuntimeType.COMPUTE;
                        break;
                }

            }

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider
                    .buildDisconnectFeedMessengerRuntime(spec, feedConnectionId, locations, subscribableRuntmeType,
                            completeDisconnection, sourceFeedId);

            feedMessenger = p.first;
            messengerPc = p.second;

        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
        spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
        spec.addRoot(nullSink);
        return new Triple<JobSpecification, Boolean, Boolean>(spec, completeDisconnection, needToDiscontinueSourceFeed);
    }
}
