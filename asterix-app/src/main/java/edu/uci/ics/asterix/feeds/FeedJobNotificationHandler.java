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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
import edu.uci.ics.asterix.common.feeds.FeedPointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.IFeedPoint;
import edu.uci.ics.asterix.common.feeds.IFeedPoint.Scope;
import edu.uci.ics.asterix.common.feeds.IFeedPoint.State;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.feeds.FeedLifecycleListener.Message;
import edu.uci.ics.asterix.file.JobSpecificationUtils;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedManagerElectMessage;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.feeds.FeedWorkManager;
import edu.uci.ics.asterix.metadata.feeds.IFeedMessage;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.runtime.formats.NonTaggedDataFormat;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraintHelper;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
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
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

public class FeedJobNotificationHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedJobNotificationHandler.class.getName());

    private LinkedBlockingQueue<Message> inbox;
    private FeedMessenger feedMessenger;
    private LinkedBlockingQueue<FeedMessengerMessage> messengerOutbox;
    private int superFeedManagerPort = 3000;
    private Executor executor = Executors.newCachedThreadPool();

    private Map<JobId, FeedSubscriber> jobSubscriberMap = new HashMap<JobId, FeedSubscriber>();
    private Map<FeedConnectionId, FeedSubscriber> connectionSubscriberMap = new HashMap<FeedConnectionId, FeedSubscriber>();
    private Map<JobId, FeedPointKey> intakeFeedPointMap = new HashMap<JobId, FeedPointKey>();
    private Map<FeedId, List<FeedPointKey>> feedPipeline = new HashMap<FeedId, List<FeedPointKey>>();
    private Map<FeedPointKey, IFeedPoint> feedPoints = new HashMap<FeedPointKey, IFeedPoint>();
    private Map<FeedConnectionId, FeedPointKey> feedConnections = new HashMap<FeedConnectionId, FeedPointKey>();
    private List<JobId> registeredJobs = new ArrayList<JobId>();

    public FeedJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
        this.inbox = inbox;
        messengerOutbox = new LinkedBlockingQueue<FeedMessengerMessage>();
        feedMessenger = new FeedMessenger(messengerOutbox);
        executor.execute(feedMessenger);
    }

    public boolean isFeedPointAvailable(FeedPointKey feedPointKey) {
        return feedPoints.containsKey(feedPointKey);
    }

    public Collection<FeedSubscriber> getSubscribers() {
        return jobSubscriberMap.values();
    }

    public Collection<IFeedPoint> getFeedIntakePoints() {
        List<IFeedPoint> intakeFeedPoints = new ArrayList<IFeedPoint>();
        for (FeedPointKey fpk : intakeFeedPointMap.values()) {
            IFeedPoint fp = feedPoints.get(fpk);
            if (fp.getType().equals(FeedPoint.Type.PRIMARY)) {
                intakeFeedPoints.add(fp);
            }
        }
        return intakeFeedPoints;
    }

    public void registerFeedPoint(IFeedPoint feedPoint) {
        if (feedPoints.containsKey(feedPoint.getFeedPointKey())) {
            throw new IllegalArgumentException("Feed point " + feedPoint + " already registered");
        }
        feedPoints.put(feedPoint.getFeedPointKey(), feedPoint);
        List<FeedPointKey> feedPointsOnPipeline = feedPipeline.get(feedPoint.getOwnerFeedId());
        if (feedPointsOnPipeline == null) {
            feedPointsOnPipeline = new ArrayList<FeedPointKey>();
            feedPipeline.put(feedPoint.getOwnerFeedId(), feedPointsOnPipeline);
        }
        feedPointsOnPipeline.add(feedPoint.getFeedPointKey());
    }

    public void deregisterFeedPoint(FeedPointKey feedPointKey) {
        if (!feedPoints.containsKey(feedPointKey)) {
            throw new IllegalArgumentException("Feed point key " + feedPointKey + " is not registered");
        }
        feedPoints.remove(feedPointKey);
        List<FeedPointKey> fps = feedPipeline.get(feedPointKey.getFeedId());
        if (fps != null && !fps.isEmpty()) {
            fps.remove(feedPointKey);
        }
    }

    public IFeedPoint getFeedPoint(FeedPointKey feedPointKey) {
        return feedPoints.get(feedPointKey);
    }

    public IFeedPoint getAvailableFeedPoint(FeedPointKey feedPointKey) {
        IFeedPoint feedPoint = feedPoints.get(feedPointKey);
        if (feedPoint == null) {
            String feedPointKeyString = feedPointKey.getStringRep();
            List<FeedPointKey> feedPointsOnFeedPipeline = feedPipeline.get(feedPointKey.getFeedId());
            FeedPointKey candidateFeedPointKey = null;
            if (feedPointsOnFeedPipeline != null) {
                for (FeedPointKey fk : feedPointsOnFeedPipeline) {
                    if (feedPointKeyString.contains(fk.getStringRep())) {
                        if (candidateFeedPointKey == null) {
                            candidateFeedPointKey = fk;
                        } else if (fk.getStringRep().contains(candidateFeedPointKey.getStringRep())) {
                            candidateFeedPointKey = fk;
                        }
                    }
                }
            }
            feedPoint = feedPoints.get(candidateFeedPointKey);
        }
        return feedPoint;
    }

    public void registerFeedCollectionJob(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobId jobId,
            JobSpecification jobSpec, Map<String, String> feedPolicy) {
        if (registeredJobs.contains(jobId)) {
            throw new IllegalStateException("Feed job already registered");
        }

        boolean found = false;
        for (Entry<FeedPointKey, IFeedPoint> entry : feedPoints.entrySet()) {
            IFeedPoint feedPoint = entry.getValue();
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

    public IFeedPoint getFeedPoint(FeedId sourceFeedId, Scope scope) {
        List<FeedPointKey> feedPointKeys = feedPipeline.get(sourceFeedId);
        for (FeedPointKey fpk : feedPointKeys) {
            IFeedPoint feedPoint = feedPoints.get(fpk);
            if (feedPoint.getScope().equals(scope)) {
                return feedPoint;
            }
        }
        return null;
    }

    public void registerFeedIntakeJob(FeedId feedId, JobId jobId, JobSpecification jobSpec) {
        if (registeredJobs.contains(jobId)) {
            throw new IllegalStateException("Feed job already registered");
        }

        boolean found = false;
        List<FeedPointKey> feedPointKeysOnPipeline = feedPipeline.get(feedId);
        IFeedPoint feedPoint = null;
        switch (feedPointKeysOnPipeline.size()) {
            case 0:
                break;
            case 1:
                feedPoint = feedPoints.get(feedPointKeysOnPipeline.get(0));
                found = true;
                break;
            case 2:
                IFeedPoint fp1 = feedPoints.get(feedPointKeysOnPipeline.get(0));
                if (fp1.getScope().equals(IFeedPoint.Scope.PRIVATE)) {
                    feedPoint = fp1;
                } else {
                    feedPoint = feedPoints.get(feedPointKeysOnPipeline.get(1));
                }
                found = true;
                break;
            default:
                found = false;
                break;
        }

        intakeFeedPointMap.put(jobId, feedPoint.getFeedPointKey());
        feedPoint.setJobId(jobId);
        feedPoint.setJobSpec(jobSpec);
        feedPoint.setState(FeedPoint.State.INITIALIZED);

        if (found) {
            registeredJobs.add(jobId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake [" + jobId + "]" + " for feed " + feedId);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not register feed intake job [" + jobId + "]" + " for feed  " + feedId);
            }
        }
    }

    public void deregisterFeedIntakeJob(FeedId feedId, JobId jobId) {
        if (!registeredJobs.contains(jobId)) {
            throw new IllegalStateException(" Feed Intake job not registered ");
        }
        intakeFeedPointMap.remove(jobId);
        registeredJobs.remove(jobId);
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
        FeedPointKey fpk = intakeFeedPointMap.get(message.jobId);
        boolean intakeJob = fpk != null;
        if (intakeJob) {
            IFeedPoint fp = feedPoints.get(fpk);
            handleFeedIntakeJobStartMessage(fp, message);
        } else {
            FeedSubscriber feedSubscriber = jobSubscriberMap.get(message.jobId);
            handleFeedCollectJobStartMessage(feedSubscriber, message);
            feedSubscriber.setStatus(FeedSubscriber.Status.ACTIVE);
        }
    }

    private void handleFeedJobFinishMessage(Message message) throws Exception {
        FeedPointKey fpk = intakeFeedPointMap.get(message.jobId);
        boolean intakeJob = fpk != null;
        if (intakeJob) {
            IFeedPoint fp = feedPoints.get(fpk);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Job finished for feed intake" + fp);
            }
            handleFeedIntakeJobFinishMessage(fp, message);
        } else {
            FeedSubscriber feedSubscriber = jobSubscriberMap.get(message.jobId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Job finished for feed feedSubscriber " + feedSubscriber);
            }
            handleFeedCollectJobFinishMessage(feedSubscriber, message);
        }
    }

    private synchronized void handleFeedIntakeJobStartMessage(IFeedPoint feedPoint, Message message) throws Exception {
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

    public synchronized void submitFeedSubscriptionRequest(IFeedPoint feedPoint, final FeedSubscriptionRequest request)
            throws Exception {
        List<String> locations = feedPoint.getLocations();
        SubscribeFeedWork work = new SubscribeFeedWork(locations.toArray(new String[] {}), request);
        FeedWorkManager.INSTANCE.submitWork(work, new SubscribeFeedWork.FeedSubscribeWorkEventListener());
    }

    public IFeedPoint getSourceFeedPoint(FeedConnectionId connectionId) {
        FeedPointKey feedPointKey = feedConnections.get(connectionId);
        if (feedPointKey != null) {
            return feedPoints.get(feedPointKey);
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
                for (IPushRuntimeFactory rf : runtimeFactories) {
                    if (rf instanceof AssignRuntimeFactory) {
                        computeOperatorIds.add(entry.getKey());
                    }
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

            List<FeedPointKey> feedPointKeysOnPipeline = feedPipeline.get(subscriber.getFeedConnectionId().getFeedId());
            for (FeedPointKey fpk : feedPointKeysOnPipeline) {
                if (fpk.equals(subscriber.getSourceFeedPointKey())) {
                    continue;
                }
                IFeedPoint fp = feedPoints.get(fpk);
                if (!fp.getState().equals(IFeedPoint.State.ACTIVE)) {
                    fp.setJobId(jobId);
                    fp.setJobSpec(jobSpec);
                    fp.setLocations(computeLocations);
                    fp.setState(State.ACTIVE);
                }
            }

            FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor(subscriber.getFeedPolicyParameters());
            if (policyAccessor.collectStatistics() || policyAccessor.isElastic()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Feed " + subscriber.getFeedConnectionId() + " requires Super Feed Manager");
                }
                configureSuperFeedManager(subscriber);
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
            } catch (Exception e) {
                MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            } finally {
                MetadataManager.INSTANCE.releaseWriteLatch();
            }

        } catch (Exception e) {
            // TODO Add Exception handling here
        }
    }

    private void configureSuperFeedManager(FeedSubscriber feedSubscriber) {
        FeedConnectionInfo feedConnectionInfo = feedSubscriber.getFeedConnectionInfo();
        int superFeedManagerIndex = new Random().nextInt(feedConnectionInfo.getCollectLocations().size());
        String superFeedManagerHost = feedConnectionInfo.getCollectLocations().get(superFeedManagerIndex);

        Cluster cluster = AsterixClusterProperties.INSTANCE.getCluster();
        String instanceName = cluster.getInstanceName();
        String node = superFeedManagerHost.substring(instanceName.length() + 1);
        String hostIp = null;
        for (Node n : cluster.getNode()) {
            if (n.getId().equals(node)) {
                hostIp = n.getClusterIp();
                break;
            }
        }
        if (hostIp == null) {
            throw new IllegalStateException("Unknown node " + superFeedManagerHost);
        }

        feedSubscriber.setSuperFeedManagerHost(hostIp);
        feedSubscriber.setSuperFeedManagerPort(superFeedManagerPort);

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Super Feed Manager for " + feedSubscriber.getFeedConnectionId() + " is " + hostIp + " node "
                    + superFeedManagerHost + "[" + superFeedManagerPort + "]");
        }

        FeedManagerElectMessage feedMessage = new FeedManagerElectMessage(hostIp, superFeedManagerHost,
                superFeedManagerPort, feedSubscriber.getFeedConnectionId());
        superFeedManagerPort += SuperFeedManager.PORT_RANGE_ASSIGNED;
        messengerOutbox.add(new FeedMessengerMessage(feedMessage, feedSubscriber));
    }

    private void handleFeedIntakeJobFinishMessage(IFeedPoint feedPoint, Message message) {
        boolean feedFailedDueToPostSubmissionNodeLoss = failedDueToNodeFalilurePostSubmission(feedPoint.getJobSpec());
        deregisterFeedIntakeJob(feedPoint.getFeedPointKey().getFeedId(), feedPoint.getJobId());
    }

    private void handleFeedCollectJobFinishMessage(FeedSubscriber subscriber, Message message) throws Exception {
        boolean feedFailedDueToPostSubmissionNodeLoss = failedDueToNodeFalilurePostSubmission(subscriber.getJobSpec());
        if (!feedFailedDueToPostSubmissionNodeLoss) {
            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(message.jobId);
            JobStatus status = info.getStatus();
            boolean failure = status != null && status.equals(JobStatus.FAILURE);
            Map<String, String> details = new HashMap<String, String>();
            if (failure) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(info + " failed on account of " + details);
                }
            } else {
                subscriber.setStatus(FeedSubscriber.Status.INACTIVE);
                feedPoints.get(subscriber.getSourceFeedPointKey()).removeSubscriber(subscriber);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Subscription " + subscriber.getFeedConnectionId()
                            + " completed successfully. Removed subscription");
                }
            }

            deregisterFeedSubscriber(subscriber);
            deregisterFeedConnection(subscriber.getFeedConnectionId());

        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Attempt to revive feed");
            }
            FeedsActivator activator = new FeedsActivator();
            FeedConnectionId connectionId = subscriber.getFeedConnectionId();
            String dataverse = connectionId.getFeedId().getDataverse();
            String datasetName = connectionId.getDatasetName();
            String feedName = connectionId.getFeedId().getFeedName();
            String feedPolicy = subscriber.getFeedPolicy();
            activator.reviveFeed(dataverse, feedName, datasetName, feedPolicy);
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Revived Feed");
            }

        }
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
        List<FeedPointKey> fpks = feedPipeline.get(connectionId.getFeedId());
        boolean hasDependents = false;
        List<FeedPointKey> candidateFPForRemoval = new ArrayList<FeedPointKey>();
        List<FeedPointKey> candidateFPForRetention = new ArrayList<FeedPointKey>();

        for (FeedPointKey fpk : fpks) {
            IFeedPoint fp = feedPoints.get(fpk);
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
                candidateFPForRemoval.add(fp.getFeedPointKey());
            } else {
                candidateFPForRetention.add(fp.getFeedPointKey());
            }
            hasDependents = false;
        }
        for (FeedPointKey fpk : candidateFPForRemoval) {
            feedPoints.remove(fpk);
        }

        feedPipeline.remove(connectionId.getFeedId());
        if (!candidateFPForRetention.isEmpty()) {
            feedPipeline.put(candidateFPForRetention.get(0).getFeedId(), candidateFPForRetention);
        }

        connectionSubscriberMap.remove(connectionId);
    }

    private void deregisterFeedSubscriber(FeedSubscriber subscriber) {
        deregisterFeedActivity(subscriber);
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
                        case SUPER_FEED_MANAGER_ELECT:
                            Thread.sleep(2000);
                            sendSuperFeedManangerElectMessage(feedSubscriber,
                                    (FeedManagerElectMessage) message.getMessage());
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

    private static void sendSuperFeedManangerElectMessage(FeedSubscriber feedSubscriber,
            FeedManagerElectMessage electMessage) {
        try {
            Dataverse dataverse = new Dataverse(feedSubscriber.getFeedConnectionId().getFeedId().getDataverse(),
                    NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT, 0);
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(dataverse);
            JobSpecification spec = JobSpecificationUtils.createJobSpecification();

            IOperatorDescriptor feedMessenger;
            AlgebricksPartitionConstraint messengerPc;
            Set<String> locations = new HashSet<String>();
            locations.addAll(feedSubscriber.getFeedConnectionInfo().getComputeLocations());
            locations.addAll(feedSubscriber.getFeedConnectionInfo().getCollectLocations());
            locations.addAll(feedSubscriber.getFeedConnectionInfo().getStorageLocations());

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider.buildSendFeedMessageRuntime(
                    spec, feedSubscriber.getFeedConnectionId(), electMessage, locations.toArray(new String[] {}));
            feedMessenger = p.first;
            messengerPc = p.second;
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, feedMessenger, messengerPc);

            NullSinkOperatorDescriptor nullSink = new NullSinkOperatorDescriptor(spec);
            AlgebricksPartitionConstraintHelper.setPartitionConstraintInJobSpec(spec, nullSink, messengerPc);
            spec.connect(new OneToOneConnectorDescriptor(spec), feedMessenger, 0, nullSink, 0);
            spec.addRoot(nullSink);

            JobId jobId = AsterixAppContextInfo.getInstance().getHcc().startJob(spec);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" Super Feed Manager Message: " + electMessage + " Job Id " + jobId);
            }

        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Exception in sending super feed manager elect message: "
                        + feedSubscriber.getFeedConnectionId() + " " + e.getMessage());
            }
        }
    }

    public boolean isRegisteredFeedJob(JobId jobId) {
        return registeredJobs.contains(jobId);
    }

    public List<String> getFeedComputeLocations(FeedId feedId) {
        return connectionSubscriberMap.get(feedId).getFeedConnectionInfo().getComputeLocations();
    }

}