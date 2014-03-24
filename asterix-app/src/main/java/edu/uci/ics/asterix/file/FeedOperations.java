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
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.IFeedJoint.Scope;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener;
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
            AqlMetadataProvider metadataProvider, FeedConnectionId connectionId) throws AsterixException,
            AlgebricksException {

        JobSpecification spec = JobSpecificationUtils.createJobSpecification();
        IOperatorDescriptor feedMessenger;
        AlgebricksPartitionConstraint messengerPc;
        boolean hasOtherSubscribers = false;
        SubscriptionLocation subscriptionLocation = null;
        try {
            IFeedJoint sourceFeedJoint = FeedLifecycleListener.INSTANCE.getSourceFeedPoint(connectionId);
            IFeedJoint subscribableFeedPoint = sourceFeedJoint;
            List<String> locations = null;
            if (sourceFeedJoint.getScope().equals(Scope.PRIVATE)) {
                // get the public feed joint on this pipeline
                IFeedJoint feedJoint = FeedLifecycleListener.INSTANCE.getFeedPoint(sourceFeedJoint.getFeedJointKey()
                        .getFeedId(), Scope.PUBLIC);
                List<FeedSubscriber> subscribers = feedJoint.getSubscribers();
                hasOtherSubscribers = subscribers != null && !subscribers.isEmpty();
                if (hasOtherSubscribers) { // if public feed joint is serving other subscribers, we disconnect partially
                    subscriptionLocation = feedJoint.getSubscriptionLocation();
                    locations = feedJoint.getLocations();
                } else { // disconnect completely
                    subscriptionLocation = sourceFeedJoint.getSubscriptionLocation();
                    locations = sourceFeedJoint.getLocations();
                }
            } else {
                List<FeedSubscriber> subscribers = sourceFeedJoint.getSubscribers();
                hasOtherSubscribers = subscribers != null && subscribers.size() > 1;
                subscriptionLocation = sourceFeedJoint.getSubscriptionLocation();
                locations = subscribableFeedPoint.getLocations();
            }

            boolean needToDiscontinueSource = !hasOtherSubscribers && sourceFeedJoint.getSubscribers().size() == 1;
            FeedRuntimeType feedRuntimeType = subscriptionLocation.equals(SubscriptionLocation.SOURCE_FEED_INTAKE) ? FeedRuntimeType.INTAKE
                    : FeedRuntimeType.COMPUTE;

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider
                    .buildDisconnectFeedMessengerRuntime(spec, connectionId, locations.toArray(new String[] {}),
                            feedRuntimeType, !hasOtherSubscribers, subscribableFeedPoint.getOwnerFeedId());

            feedMessenger = p.first;
            messengerPc = p.second;

            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);
            NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
            spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
            spec.addRoot(nullSink);
            return new Triple<JobSpecification, Boolean, Boolean>(spec, hasOtherSubscribers, needToDiscontinueSource);

        } catch (AlgebricksException e) {
            throw new AsterixException(e);
        }

    }
}
