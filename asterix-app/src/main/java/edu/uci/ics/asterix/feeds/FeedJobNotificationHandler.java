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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.api.common.FeedWorkCollection.SubscribeFeedWork;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedConnectionInfo;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.IFeedJoint.Scope;
import edu.uci.ics.asterix.common.feeds.IFeedJoint.State;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleEventSubscriber;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleEventSubscriber.FeedLifecycleEvent;
import edu.uci.ics.asterix.common.feeds.IFeedMessage;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener.Message;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedWorkManager;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.runtime.operators.meta.AlgebricksMetaOperatorDescriptor;
import edu.uci.ics.hyracks.algebricks.runtime.operators.std.AssignRuntimeFactory;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

public class FeedJobNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());

    private final Executor executor = Executors.newCachedThreadPool();
    private final LinkedBlockingQueue<Message> inbox;
    private final FeedMessenger feedMessenger;
    private final LinkedBlockingQueue<FeedMessengerMessage> messengerOutbox;
    private final Map<JobId, FeedSubscriber> jobSubscriberMap;
    private final Map<FeedConnectionId, FeedSubscriber> connectionSubscriberMap;

    private final Map<JobId, FeedJointKey> intakeFeedJointMap;
    private final Map<FeedId, List<FeedJointKey>> feedPipeline;
    private final Map<FeedJointKey, IFeedJoint> feedJoints;

    private final Map<FeedConnectionId, FeedJointKey> feedConnections;
    private final List<JobId> registeredJobs;
    private final Map<FeedConnectionId, List<IFeedLifecycleEventSubscriber>> registeredFeedEventSubscribers;

    public FeedJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
        this.inbox = inbox;
        this.messengerOutbox = new LinkedBlockingQueue<FeedMessengerMessage>();
        this.feedMessenger = new FeedMessenger(messengerOutbox);
        this.executor.execute(feedMessenger);
        this.jobSubscriberMap = new LinkedHashMap<JobId, FeedSubscriber>();
        this.connectionSubscriberMap = new HashMap<FeedConnectionId, FeedSubscriber>();
        this.intakeFeedJointMap = new HashMap<JobId, FeedJointKey>();
        this.feedPipeline = new HashMap<FeedId, List<FeedJointKey>>();
        this.feedJoints = new HashMap<FeedJointKey, IFeedJoint>();
        this.feedConnections = new HashMap<FeedConnectionId, FeedJointKey>();
        this.registeredJobs = new ArrayList<JobId>();
        this.registeredFeedEventSubscribers = new HashMap<FeedConnectionId, List<IFeedLifecycleEventSubscriber>>();
    }

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        List<IFeedLifecycleEventSubscriber> subscribers = registeredFeedEventSubscribers.get(connectionId);
        if (subscribers == null) {
            subscribers = new ArrayList<IFeedLifecycleEventSubscriber>();
            registeredFeedEventSubscribers.put(connectionId, subscribers);
        }
        subscribers.add(subscriber);
    }

    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        List<IFeedLifecycleEventSubscriber> subscribers = registeredFeedEventSubscribers.get(connectionId);
        if (subscribers != null) {
            subscribers.remove(subscriber);
        }
    }

    public boolean isFeedPointAvailable(FeedJointKey feedPointKey) {
        return feedJoints.containsKey(feedPointKey);
    }

    public Collection<FeedSubscriber> getSubscribers() {
        return jobSubscriberMap.values();
    }

    public Collection<IFeedJoint> getFeedIntakePoints() {
        List<IFeedJoint> intakeFeedPoints = new ArrayList<IFeedJoint>();
        for (FeedJointKey fpk : intakeFeedJointMap.values()) {
            IFeedJoint fp = feedJoints.get(fpk);
            if (fp != null) {
                if (fp.getType().equals(FeedJoint.Type.PRIMARY)) {
                    intakeFeedPoints.add(fp);
                }
            }
        }
        return intakeFeedPoints;
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        if (feedJoints.containsKey(feedJoint.getFeedJointKey())) {
            throw new IllegalArgumentException("Feed point " + feedJoint + " already registered");
        }
        feedJoints.put(feedJoint.getFeedJointKey(), feedJoint);
        List<FeedJointKey> feedJointsOnPipeline = feedPipeline.get(feedJoint.getOwnerFeedId());
        if (feedJointsOnPipeline == null) {
            feedJointsOnPipeline = new ArrayList<FeedJointKey>();
            feedPipeline.put(feedJoint.getOwnerFeedId(), feedJointsOnPipeline);
        }
        feedJointsOnPipeline.add(feedJoint.getFeedJointKey());
    }

    /*
    public void deregisterFeedJoint(FeedJointKey feedJointKey) {
        if (!feedJoints.containsKey(feedJointKey)) {
            throw new IllegalArgumentException("Feed point key " + feedJointKey + " is not registered");
        }
        feedJoints.remove(feedJointKey);
        List<FeedJointKey> fps = feedPipeline.get(feedJointKey.getFeedId());
        if (fps != null && !fps.isEmpty()) {
            fps.remove(feedJointKey);
        }
    }*/

    public IFeedJoint getFeedJoint(FeedJointKey feedPointKey) {
        return feedJoints.get(feedPointKey);
    }

    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedPointKey) {
        IFeedJoint feedPoint = feedJoints.get(feedPointKey);
        if (feedPoint == null) {
            String feedPointKeyString = feedPointKey.getStringRep();
            List<FeedJointKey> feedPointsOnFeedPipeline = feedPipeline.get(feedPointKey.getFeedId());
            FeedJointKey candidateFeedPointKey = null;
            if (feedPointsOnFeedPipeline != null) {
                for (FeedJointKey fk : feedPointsOnFeedPipeline) {
                    if (feedPointKeyString.contains(fk.getStringRep())) {
                        if (candidateFeedPointKey == null) {
                            candidateFeedPointKey = fk;
                        } else if (fk.getStringRep().contains(candidateFeedPointKey.getStringRep())) { // found feed point is a super set of the earlier find
                            candidateFeedPointKey = fk;
                        }
                    }
                }
            }
            feedPoint = feedJoints.get(candidateFeedPointKey);
        }
        return feedPoint;
    }

    public void registerFeedCollectionJob(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (registeredJobs.contains(jobId)) {
            throw new IllegalStateException("Feed job already registered");
        }

        boolean found = false;
        for (Entry<FeedJointKey, IFeedJoint> entry : feedJoints.entrySet()) {
            IFeedJoint feedPoint = entry.getValue();
            FeedSubscriber subscriber = feedPoint.getSubscriber(feedConnectionId);
            if (subscriber != null) {
                subscriber.setJobId(jobId);
                subscriber.setJobSpec(jobSpec);
                subscriber.setStatus(FeedSubscriber.Status.INIITIALIZED);
                jobSubscriberMap.put(jobId, subscriber);
                connectionSubscriberMap.put(feedConnectionId, subscriber);
                feedConnections.put(feedConnectionId, subscriber.getSourceFeedPointKey());
                found = true;
                break;
            }
        }

        if (found) {
            registeredJobs.add(jobId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed connection [" + jobId + "]" + " for feed " + feedConnectionId);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not register feed collection job [" + jobId + "]" + " for feed connection "
                        + feedConnectionId);
            }
        }

    }

    public IFeedJoint getFeedPoint(FeedId sourceFeedId, Scope scope) {
        List<FeedJointKey> feedPointKeys = feedPipeline.get(sourceFeedId);
        for (FeedJointKey fpk : feedPointKeys) {
            IFeedJoint feedPoint = feedJoints.get(fpk);
            if (feedPoint.getScope().equals(scope)) {
                return feedPoint;
            }
        }
        return null;
    }

    public Map<FeedJointKey, IFeedJoint> getFeedPoints() {
        return feedJoints;
    }

    public void registerFeedIntakeJob(FeedId feedId, JobId jobId, JobSpecification jobSpec) throws HyracksDataException {
        if (registeredJobs.contains(jobId)) {
            throw new IllegalStateException("Feed job already registered");
        }

        boolean found = true;
        List<FeedJointKey> feedJointKeysOnPipeline = feedPipeline.get(feedId);
        IFeedJoint feedJoint = null;
        switch (feedJointKeysOnPipeline.size()) {
            case 1:
                feedJoint = feedJoints.get(feedJointKeysOnPipeline.get(0));
                break;
            case 2:
                IFeedJoint fp1 = feedJoints.get(feedJointKeysOnPipeline.get(0));
                if (fp1.getScope().equals(IFeedJoint.Scope.PRIVATE)) {
                    feedJoint = fp1;
                } else {
                    feedJoint = feedJoints.get(feedJointKeysOnPipeline.get(1));
                }
                break;
            default:
                found = false;
                break;
        }

        if (found) {
            intakeFeedJointMap.put(jobId, feedJoint.getFeedJointKey());
            feedJoint.setJobId(jobId);
            feedJoint.setJobSpec(jobSpec);
            feedJoint.setState(FeedJoint.State.INITIALIZED);

            registeredJobs.add(jobId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not register feed intake job [" + jobId + "]" + " for feed  " + feedId);
            }
            throw new HyracksDataException("Could not register feed intake job [" + jobId + "]" + " for feed  "
                    + feedId);
        }
    }

    public void deregisterFeedIntakeJob(JobId jobId) {
        if (!registeredJobs.contains(jobId)) {
            throw new IllegalStateException(" Feed Intake job not registered ");
        }
        FeedJointKey feedJointKey = intakeFeedJointMap.remove(jobId);
        IFeedJoint feedJoint = feedJoints.remove(feedJointKey);
        registeredJobs.remove(jobId);
        if (feedJoint != null) {
            List<FeedJointKey> feedJointsOnPipeline = feedPipeline.get(feedJoint.getFeedJointKey().getFeedId());
            if (feedJointsOnPipeline != null) {
                feedJointsOnPipeline.remove(feedJointKey);
            }
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deregistered feed intake job [" + jobId + "]");
        }
    }

    @Override
    public void run() {
        Message mesg;
        while (true) {
            try {
                mesg = inbox.take();
                switch (mesg.messageKind) {
                    case JOB_START:
                        handleFeedJobStartMessage(mesg);
                        break;
                    case JOB_FINISH:
                        handleFeedJobFinishMessage(mesg);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private void handleFeedJobStartMessage(Message message) throws Exception {
        FeedJointKey fpk = intakeFeedJointMap.get(message.jobId);
        boolean intakeJob = fpk != null;
        if (intakeJob) {
            IFeedJoint fp = feedJoints.get(fpk);
            handleFeedIntakeJobStartMessage(fp, message);
        } else {
            FeedSubscriber feedSubscriber = jobSubscriberMap.get(message.jobId);
            handleFeedCollectJobStartMessage(feedSubscriber, message);
            feedSubscriber.setStatus(FeedSubscriber.Status.ACTIVE);
        }
    }

    private void handleFeedJobFinishMessage(Message message) throws Exception {
        FeedJointKey fpk = intakeFeedJointMap.get(message.jobId);
        boolean intakeJob = fpk != null;
        if (intakeJob) {
            IFeedJoint fp = feedJoints.get(fpk);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Intake Job finished for feed intake " + fp);
            }
            handleFeedIntakeJobFinishMessage(message);
        } else {
            FeedSubscriber feedSubscriber = jobSubscriberMap.get(message.jobId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Collect Job finished for feed feedSubscriber " + feedSubscriber);
            }
            handleFeedCollectJobFinishMessage(feedSubscriber, message);
        }
    }

    private synchronized void handleFeedIntakeJobStartMessage(IFeedJoint feedPoint, Message message) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Job started for FeedPoint " + feedPoint);
        }
        List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<OperatorDescriptorId>();
        Map<OperatorDescriptorId, IOperatorDescriptor> operators = feedPoint.getJobSpec().getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                intakeOperatorIds.add(opDesc.getOperatorId());
            }
        }

        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        List<String> intakeLocations = new ArrayList<String>();
        for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
            Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
            int nOperatorInstances = operatorLocations.size();
            for (int i = 0; i < nOperatorInstances; i++) {
                intakeLocations.add(operatorLocations.get(i));
            }
        }

        feedPoint.setLocations(intakeLocations);
        feedPoint.setState(State.ACTIVE);
    }

    public synchronized void submitFeedSubscriptionRequest(IFeedJoint feedPoint, final FeedSubscriptionRequest request)
            throws Exception {
        List<String> locations = feedPoint.getLocations();
        SubscribeFeedWork work = new SubscribeFeedWork(locations.toArray(new String[] {}), request);
        FeedWorkManager.INSTANCE.submitWork(work, new SubscribeFeedWork.FeedSubscribeWorkEventListener());
    }

    public IFeedJoint getSourceFeedPoint(FeedConnectionId connectionId) {
        FeedJointKey feedPointKey = feedConnections.get(connectionId);
        if (feedPointKey != null) {
            return feedJoints.get(feedPointKey);
        }
        return null;
    }

    public Set<FeedConnectionId> getActiveFeedConnections() {
        return feedConnections.keySet();
    }

    public boolean isFeedConnectionActive(FeedConnectionId connectionId) {
        return feedConnections.get(connectionId) != null;
    }

    private void handleFeedCollectJobStartMessage(FeedSubscriber subscriber, Message message) {
        JobId jobId = message.jobId;

        JobSpecification jobSpec = subscriber.getJobSpec();

        List<OperatorDescriptorId> collectOperatorIds = new ArrayList<OperatorDescriptorId>();
        List<OperatorDescriptorId> computeOperatorIds = new ArrayList<OperatorDescriptorId>();
        List<OperatorDescriptorId> storageOperatorIds = new ArrayList<OperatorDescriptorId>();

        Map<OperatorDescriptorId, IOperatorDescriptor> operators = jobSpec.getOperatorMap();
        for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
            IOperatorDescriptor opDesc = entry.getValue();
            IOperatorDescriptor actualOp = null;
            if (opDesc instanceof FeedMetaOperatorDescriptor) {
                actualOp = ((FeedMetaOperatorDescriptor) opDesc).getCoreOperator();
            } else {
                actualOp = opDesc;
            }

            if (actualOp instanceof AlgebricksMetaOperatorDescriptor) {
                AlgebricksMetaOperatorDescriptor op = ((AlgebricksMetaOperatorDescriptor) actualOp);
                IPushRuntimeFactory[] runtimeFactories = op.getPipeline().getRuntimeFactories();
                boolean computeOp = false;
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof AssignRuntimeFactory) {
                        computeOp = true;
                        break;
                    }
                }
                if (computeOp) {
                    computeOperatorIds.add(entry.getKey());
                }
            } else if (actualOp instanceof LSMTreeIndexInsertUpdateDeleteOperatorDescriptor) {
                storageOperatorIds.add(entry.getKey());
            } else if (actualOp instanceof FeedCollectOperatorDescriptor) {
                collectOperatorIds.add(entry.getKey());
            }
        }

        try {
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(message.jobId);
            List<String> collectLocations = new ArrayList<String>();
            for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    collectLocations.add(operatorLocations.get(i));
                }
            }

            List<String> computeLocations = new ArrayList<String>();
            for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(computeOpId);
                if (operatorLocations != null) {
                    int nOperatorInstances = operatorLocations.size();
                    for (int i = 0; i < nOperatorInstances; i++) {
                        computeLocations.add(operatorLocations.get(i));
                    }
                } else {
                    computeLocations.clear();
                    computeLocations.addAll(collectLocations);
                }
            }

            List<String> storageLocations = new ArrayList<String>();
            for (OperatorDescriptorId storageOpId : storageOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(storageOpId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    storageLocations.add(operatorLocations.get(i));
                }
            }

            FeedConnectionInfo connectionInfo = new FeedConnectionInfo(subscriber.getFeedConnectionId(),
                    collectLocations, computeLocations, storageLocations);
            subscriber.setFeedConnectionInfo(connectionInfo);

            List<FeedJointKey> feedPointKeysOnPipeline = feedPipeline.get(subscriber.getFeedConnectionId().getFeedId());
            for (FeedJointKey fpk : feedPointKeysOnPipeline) {
                if (fpk.equals(subscriber.getSourceFeedPointKey())) {
                    continue;
                }
                IFeedJoint fp = feedJoints.get(fpk);
                fp.setJobId(jobId);
                fp.setJobSpec(jobSpec);
                fp.setLocations(computeLocations);
                fp.setState(State.ACTIVE);
            }

            Map<String, String> feedActivityDetails = new HashMap<String, String>();
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.INTAKE_LOCATIONS,
                    StringUtils.join(connectionInfo.getCollectLocations().iterator(), ','));
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.COMPUTE_LOCATIONS,
                    StringUtils.join(connectionInfo.getComputeLocations().iterator(), ','));
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.STORAGE_LOCATIONS,
                    StringUtils.join(connectionInfo.getStorageLocations().iterator(), ','));
            String policyName = subscriber.getFeedPolicy();
            feedActivityDetails.put(FeedActivity.FeedActivityDetails.FEED_POLICY_NAME, policyName);

            MetadataManager.INSTANCE.acquireWriteLatch();
            MetadataTransactionContext mdTxnCtx = null;
            try {
                mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                FeedActivityType nextState = FeedActivityType.FEED_BEGIN;
                FeedActivity feedActivity = new FeedActivity(connectionInfo.getFeedConnectionId().getFeedId()
                        .getDataverse(), connectionInfo.getFeedConnectionId().getFeedId().getFeedName(), connectionInfo
                        .getFeedConnectionId().getDatasetName(), nextState, feedActivityDetails);
                MetadataManager.INSTANCE.registerFeedActivity(mdTxnCtx, connectionInfo.getFeedConnectionId(),
                        feedActivity);
                MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
                List<IFeedLifecycleEventSubscriber> eventSubscribers = registeredFeedEventSubscribers
                        .get(connectionInfo.getFeedConnectionId());
                if (eventSubscribers != null) {
                    for (IFeedLifecycleEventSubscriber eventSubscriber : eventSubscribers) {
                        eventSubscriber.handleFeedEvent(FeedLifecycleEvent.FEED_STARTED);
                    }
                }
            } catch (Exception e) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } finally {
                MetadataManager.INSTANCE.releaseWriteLatch();
            }

        } catch (Exception e) {
            // TODO Add Exception handling here
        }
    }

    private void handleFeedIntakeJobFinishMessage(Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        JobStatus status = info.getStatus();
        if (!status.equals(JobStatus.FAILURE)) {
            deregisterFeedIntakeJob(message.jobId);
        }
    }

    private void handleFeedCollectJobFinishMessage(FeedSubscriber subscriber, Message message) throws Exception {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        JobInfo info = hcc.getJobInfo(message.jobId);
        JobStatus status = info.getStatus();
        boolean failure = status != null && status.equals(JobStatus.FAILURE);
        FeedPolicyAccessor fpa = new FeedPolicyAccessor(subscriber.getFeedPolicyParameters());
        Map<String, String> details = new HashMap<String, String>();
        if (failure) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(info + " failed on account of " + details);
            }
        } else {
            subscriber.setStatus(FeedSubscriber.Status.INACTIVE);
            IFeedJoint feedJoint = feedJoints.get(subscriber.getSourceFeedPointKey());
            if (feedJoint != null) {
                feedJoint.removeSubscriber(subscriber);
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Subscription " + subscriber.getFeedConnectionId()
                        + " completed successfully. Removed subscription");
            }
        }

        if (!failure || !fpa.continueOnHardwareFailure()) {
            deregisterFeedSubscriber(subscriber);
            deregisterFeedConnection(subscriber.getFeedConnectionId());
        } else {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Not deregistering subscriber " + subscriber + "as subscriber's policy "
                        + subscriber.getFeedPolicy() + " requires recovery from failure");
            }
        }
        deregisterFeedActivity(subscriber);
    }

    private void deregisterFeedActivity(FeedSubscriber subscriber) {
        MetadataTransactionContext mdTxnCtx = null;
        boolean latchAcquired = false;
        try {
            mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
            MetadataManager.INSTANCE.acquireWriteLatch();
            latchAcquired = true;
            MetadataManager.INSTANCE.deregisterFeedActivity(mdTxnCtx, subscriber.getFeedConnectionId());
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            if (mdTxnCtx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
                } catch (RemoteException | ACIDException ae) {
                    throw new IllegalStateException(" Unable to abort ");
                }
            }
        } finally {
            if (latchAcquired) {
                MetadataManager.INSTANCE.releaseWriteLatch();
            }
        }
    }

    public void deregisterFeedConnection(FeedConnectionId connectionId) {
        feedConnections.remove(connectionId);
        List<FeedJointKey> fpks = feedPipeline.get(connectionId.getFeedId());
        boolean hasDependents = false;
        List<FeedJointKey> candidateFPForRemoval = new ArrayList<FeedJointKey>();
        List<FeedJointKey> feedJointsForRetention = new ArrayList<FeedJointKey>();

        if (fpks != null) {
            for (FeedJointKey fpk : fpks) {
                IFeedJoint fp = feedJoints.get(fpk);
                if (fp == null) {
                    continue;
                }
                List<FeedSubscriber> subscribers = fp.getSubscribers();
                if (subscribers != null && !subscribers.isEmpty()) {
                    for (FeedSubscriber subscriber : subscribers) {
                        if (!subscriber.getFeedConnectionId().getFeedId().equals(connectionId.getFeedId())
                                || !subscriber.getFeedConnectionId().equals(connectionId)) {
                            hasDependents = true;
                            break;
                        }
                    }
                }
                if (!hasDependents) {
                    candidateFPForRemoval.add(fp.getFeedJointKey());
                } else {
                    feedJointsForRetention.add(fp.getFeedJointKey());
                }
                hasDependents = false;
            }

            List<FeedJointKey> feedJointsToBeRetained = new ArrayList<FeedJointKey>();
            for (FeedJointKey key : feedJointsForRetention) {
                String stringRep = key.getStringRep();
                for (FeedJointKey fpk : candidateFPForRemoval) {
                    String rep = fpk.getStringRep();
                    if (stringRep.startsWith(rep)) {
                        feedJointsToBeRetained.add(fpk);
                    }
                }
            }
            candidateFPForRemoval.removeAll(feedJointsToBeRetained);
            for (FeedJointKey fpk : candidateFPForRemoval) {
                feedJoints.remove(fpk);
            }

            feedPipeline.remove(connectionId.getFeedId());
            if (!feedJointsForRetention.isEmpty()) {
                feedPipeline.put(feedJointsForRetention.get(0).getFeedId(), feedJointsForRetention);
            }
        }

        connectionSubscriberMap.remove(connectionId);
        List<IFeedLifecycleEventSubscriber> eventSubscribers = registeredFeedEventSubscribers.get(connectionId);
        if (eventSubscribers != null) {
            for (IFeedLifecycleEventSubscriber eventSubscriber : eventSubscribers) {
                eventSubscriber.handleFeedEvent(FeedLifecycleEvent.FEED_ENDED);
            }
        }

    }

    private void deregisterFeedSubscriber(FeedSubscriber subscriber) {
        jobSubscriberMap.remove(subscriber.getJobId());
        registeredJobs.remove(subscriber.getJobId());
    }

    private boolean failedDueToNodeFalilurePostSubmission(JobSpecification spec) {
        Set<Constraint> userConstraints = spec.getUserConstraints();
        List<String> locations = new ArrayList<String>();
        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            switch (lexpr.getTag()) {
                case PARTITION_LOCATION:
                    String location = (String) ((ConstantExpression) cexpr).getValue();
                    locations.add(location);
                    break;
            }
        }
        Set<String> participantNodes = AsterixClusterProperties.INSTANCE.getParticipantNodes();
        List<String> nodesFailedPostSubmission = new ArrayList<String>();
        for (String location : locations) {
            if (!participantNodes.contains(location)) {
                nodesFailedPostSubmission.add(location);
            }
        }

        if (nodesFailedPostSubmission.size() > 0) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed failed as nodes failed post submission");
            }
            return true;
        } else {
            return false;
        }

    }

    public static class FeedMessengerMessage {
        private final IFeedMessage message;
        private final FeedSubscriber feedSubscriber;

        public FeedMessengerMessage(IFeedMessage message, FeedSubscriber feedSubscriber) {
            this.message = message;
            this.feedSubscriber = feedSubscriber;
        }

        public IFeedMessage getMessage() {
            return message;
        }

        public FeedSubscriber getFeedSubscriber() {
            return feedSubscriber;
        }
    }

    private static class FeedMessenger implements Runnable {

        private final LinkedBlockingQueue<FeedMessengerMessage> inbox;

        public FeedMessenger(LinkedBlockingQueue<FeedMessengerMessage> inbox) {
            this.inbox = inbox;
        }

        public void run() {
            while (true) {
                FeedMessengerMessage message = null;
                try {
                    message = inbox.take();
                    FeedSubscriber feedSubscriber = message.getFeedSubscriber();
                    switch (message.getMessage().getMessageType()) {
                        case END:
                            Thread.sleep(2000);
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Sent super feed manager election message" + message.getMessage());
                            }
                    }
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }

    }

    public boolean isRegisteredFeedJob(JobId jobId) {
        return registeredJobs.contains(jobId);
    }

    public List<String> getFeedComputeLocations(FeedId feedId) {
        List<FeedJointKey> feedJointKeys = feedPipeline.get(feedId);
        for (FeedJointKey fjk : feedJointKeys) {
            if (fjk.getFeedId().equals(feedId)) {
                IFeedJoint fj = feedJoints.get(fjk);
                return fj.getLocations();
            }
        }
        return null;
    }

    public List<String> getFeedStorageLocations(FeedConnectionId connectionId) {
        return connectionSubscriberMap.get(connectionId).getFeedConnectionInfo().getStorageLocations();
    }
}