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

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.api.IFeedJoint.Scope;
import edu.uci.ics.asterix.common.feeds.api.IFeedLifecycleListener.SubscriptionLocation;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.PrimaryFeed;
import edu.uci.ics.asterix.metadata.feeds.EndFeedMessage;
import edu.uci.ics.asterix.metadata.feeds.FeedMessageOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.PrepareStallMessage;
import edu.uci.ics.asterix.metadata.feeds.TerminateDataFlowMessage;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
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

    /**
     * Builds the job spec for ingesting a (primary) feed from its external source via the feed adaptor.
     * 
     * @param primaryFeed
     * @param metadataProvider
     * @return JobSpecification the Hyracks job specification for receiving data from external source
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
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = buildDiscontinueFeedMessengerRuntime(spec, feedId,
                locations);

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
            IFeedJoint sourceFeedJoint = FeedLifecycleListener.INSTANCE.getSourceFeedJoint(connectionId);
            IFeedJoint subscribableFeedPoint = sourceFeedJoint;
            List<String> locations = null;
            if (sourceFeedJoint.getScope().equals(Scope.PRIVATE)) {
                // get the public feed joint on this pipeline
                IFeedJoint feedJoint = FeedLifecycleListener.INSTANCE.getFeedJoint(sourceFeedJoint.getFeedJointKey()
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

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = buildDisconnectFeedMessengerRuntime(spec,
                    connectionId, locations.toArray(new String[] {}), feedRuntimeType, !hasOtherSubscribers,
                    subscribableFeedPoint.getOwnerFeedId());

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

    public static JobSpecification buildPrepareStallMessageJob(PrepareStallMessage stallMessage,
            String[] collectLocations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, stallMessage.getConnectionId(), stallMessage, collectLocations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static JobSpecification buildTerminateFlowMessageJob(TerminateDataFlowMessage terminateMessage,
            String[] collectLocations) throws AsterixException {
        JobSpecification messageJobSpec = JobSpecificationUtils.createJobSpecification();
        try {
            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = FeedOperations.buildSendFeedMessageRuntime(
                    messageJobSpec, terminateMessage.getConnectionId(), terminateMessage, collectLocations);
            buildSendFeedMessageJobSpec(p.first, p.second, messageJobSpec);
        } catch (AlgebricksException ae) {
            throw new AsterixException(ae);
        }
        return messageJobSpec;
    }

    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDiscontinueFeedMessengerRuntime(
            JobSpecification jobSpec, FeedId feedId, String[] locations) throws AlgebricksException {
        FeedConnectionId feedConnectionId = new FeedConnectionId(feedId, null);
        IFeedMessage feedMessage = new EndFeedMessage(feedConnectionId, FeedRuntimeType.INTAKE,
                feedConnectionId.getFeedId(), true, EndFeedMessage.EndMessageType.DISCONTINUE_SOURCE);
        return buildSendFeedMessageRuntime(jobSpec, feedConnectionId, feedMessage, locations);
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildSendFeedMessageRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, IFeedMessage feedMessage, String[] locations)
            throws AlgebricksException {
        AlgebricksPartitionConstraint partitionConstraint = new AlgebricksAbsolutePartitionConstraint(locations);
        FeedMessageOperatorDescriptor feedMessenger = new FeedMessageOperatorDescriptor(jobSpec, feedConenctionId,
                feedMessage);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(feedMessenger, partitionConstraint);
    }

    private static JobSpecification buildSendFeedMessageJobSpec(IOperatorDescriptor operatorDescriptor,
            AlgebricksPartitionConstraint messengerPc, JobSpecification messageJobSpec) {
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(messageJobSpec, operatorDescriptor,
                messengerPc);
        NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(messageJobSpec);
        AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(messageJobSpec, nullSink, messengerPc);
        messageJobSpec.connect(new OneToOneConnectorDescriptor(messageJobSpec), operatorDescriptor, 0, nullSink, 0);
        messageJobSpec.addRoot(nullSink);
        return messageJobSpec;
    }

    private static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildDisconnectFeedMessengerRuntime(
            JobSpecification jobSpec, FeedConnectionId feedConenctionId, String[] locations,
            FeedRuntimeType sourceFeedRuntimeType, boolean completeDisconnection, FeedId sourceFeedId)
            throws AlgebricksException {
        IFeedMessage feedMessage = new EndFeedMessage(feedConenctionId, sourceFeedRuntimeType, sourceFeedId,
                completeDisconnection, EndFeedMessage.EndMessageType.DISCONNECT_FEED);
        return buildSendFeedMessageRuntime(jobSpec, feedConenctionId, feedMessage, locations);
    }
}
