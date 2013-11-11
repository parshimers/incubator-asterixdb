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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.io.PrintWriter;
import java.io.Serializable;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.FeedWorkCollection.SubscribeFeedWork;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.ConnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DisconnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager;
import edu.uci.ics.asterix.event.schema.cluster.Cluster;
import edu.uci.ics.asterix.event.schema.cluster.Node;
import edu.uci.ics.asterix.file.JobSpecificationUtils;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedFailure.FailureType;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityType;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.feeds.BuiltinFeedPolicies;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedManagerElectMessage;
import edu.uci.ics.asterix.metadata.feeds.FeedMetaOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.metadata.feeds.FeedWorkManager;
import edu.uci.ics.asterix.metadata.feeds.IFeedMessage;
import edu.uci.ics.asterix.metadata.feeds.MessageListener;
import edu.uci.ics.asterix.metadata.feeds.MessageListener.IMessageAnalyzer;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties.State;
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
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.IJobLifecycleListener;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.misc.NullSinkOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;

/**
 * A listener that subscribes to events associated with cluster membership (nodes joining/leaving the cluster)
 * and job lifecycle (start/end of a job). Subscription to such events allows keeping track of feed ingestion jobs
 * and take any corrective action that may be required when a node involved in a feed leaves the cluster.
 */
public class FeedLifecycleListener implements IJobLifecycleListener, IClusterEventsSubscriber, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(FeedLifecycleListener.class.getName());

    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    public static final int FEED_HEALTH_PORT = 2999;

    private LinkedBlockingQueue<Message> jobEventInbox;
    private LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private Map<FeedCollectInfo, List<String>> dependentFeeds = new HashMap<FeedCollectInfo, List<String>>();
    private IMessageAnalyzer healthDataParser;
    private MessageListener feedHealthDataListener;
    private ExecutorService executorService = Executors.newCachedThreadPool();
    private Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
    private State state;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final FeedWorkRequestResponseHandler feedWorkRequestResponseHandler;

    private FeedLifecycleListener() {
        jobEventInbox = new LinkedBlockingQueue<Message>();
        feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        feedWorkRequestResponseHandler = new FeedWorkRequestResponseHandler(responseInbox, feedJobNotificationHandler);
        this.healthDataParser = new FeedHealthDataParser();
        this.feedHealthDataListener = new MessageListener(FEED_HEALTH_PORT, healthDataParser.getMessageQueue());
        this.feedHealthDataListener.start();
        this.executorService.execute(feedJobNotificationHandler);
        this.executorService.execute(feedWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        state = AsterixClusterProperties.INSTANCE.getState();
    }

    public FeedIntakeInfo getFeedIntakeInfo(FeedId feedId) {
        return feedJobNotificationHandler.getFeedIntakeInfo(feedId);
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_START));
        }
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        if (feedJobNotificationHandler.isRegisteredFeedJob(jobId)) {
            jobEventInbox.add(new Message(jobId, Message.MessageKind.JOB_FINISH));
        }
    }

    @Override
    public void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf) throws HyracksException {
        JobSpecification spec = acggf.getJobSpecification();
        boolean feedIntakeJob = false;
        boolean feedCollectionJob = false;
        FeedConnectionId feedConnectionId = null;
        FeedId feedId = null;
        FeedId sourceFeedId = null;
        Map<String, String> feedPolicy = null;
        for (IOperatorDescriptor opDesc : spec.getOperatorMap().values()) {
            if (opDesc instanceof FeedCollectOperatorDescriptor) {
                feedConnectionId = ((FeedCollectOperatorDescriptor) opDesc).getFeedConnectionId();
                feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicy();
                sourceFeedId = ((FeedCollectOperatorDescriptor) opDesc).getSourceFeedId();
                feedCollectionJob = true;
                break;
            } else if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                feedId = ((FeedIntakeOperatorDescriptor) opDesc).getFeedId();
                feedIntakeJob = true;
                break;
            }
        }
        if (feedCollectionJob) {
            feedJobNotificationHandler.registerFeedConnectionJob(sourceFeedId, feedConnectionId, jobId, spec,
                    feedPolicy);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed connection: " + feedConnectionId + " ingestion policy "
                        + feedPolicy.get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY));
            }
        } else if (feedIntakeJob) {
            feedJobNotificationHandler.registerFeedIntakeJob(feedId, jobId, spec);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake job for feed: " + feedId);
            }
        }

    }

    public void registerFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.put(feedId, queue);
    }

    public void deregisterFeedReportQueue(FeedConnectionId feedId, LinkedBlockingQueue<String> queue) {
        feedReportQueue.remove(feedId);
    }

    public LinkedBlockingQueue<String> getFeedReportQueue(FeedConnectionId feedId) {
        return feedReportQueue.get(feedId);
    }

    private static class Message {
        public JobId jobId;

        public enum MessageKind {
            JOB_START,
            JOB_FINISH
        }

        public MessageKind messageKind;

        public Message(JobId jobId, MessageKind msgKind) {
            this.jobId = jobId;
            this.messageKind = msgKind;
        }
    }

    public static class FeedFailureReport {
        public Map<FeedCollectInfo, List<FeedFailure>> failures = new HashMap<FeedCollectInfo, List<FeedFailure>>();

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<FeedLifecycleListener.FeedCollectInfo, List<FeedLifecycleListener.FeedFailure>> entry : failures
                    .entrySet()) {
                builder.append(entry.getKey() + " -> failures");
                for (FeedFailure failure : entry.getValue()) {
                    builder.append("failure -> " + failure);
                }
            }
            return builder.toString();
        }
    }

    private static class FeedHealthDataParser implements IMessageAnalyzer {

        private LinkedBlockingQueue<String> inbox = new LinkedBlockingQueue<String>();

        @Override
        public LinkedBlockingQueue<String> getMessageQueue() {
            return inbox;
        }

    }

    public static class FeedJobNotificationHandler implements Runnable, Serializable {

        private static final long serialVersionUID = 1L;

        private LinkedBlockingQueue<Message> inbox;
        private Map<JobId, FeedCollectInfo> registeredFeedConnections = new HashMap<JobId, FeedCollectInfo>();
        private Map<JobId, FeedIntakeInfo> registeredFeedIntakeJobs = new HashMap<JobId, FeedIntakeInfo>();
        private Map<FeedId, FeedIntakeInfo> feedIntakeInfos = new HashMap<FeedId, FeedIntakeInfo>();
        private Map<FeedConnectionId, FeedCollectInfo> feedCollectInfos = new HashMap<FeedConnectionId, FeedCollectInfo>();
        private FeedMessenger feedMessenger;
        private LinkedBlockingQueue<FeedMessengerMessage> messengerOutbox;
        private int superFeedManagerPort = 3000;
        public Map<FeedId, List<FeedSubscriptionRequest>> subscriptions = new HashMap<FeedId, List<FeedSubscriptionRequest>>();

        public FeedJobNotificationHandler(LinkedBlockingQueue<Message> inbox) {
            this.inbox = inbox;
            messengerOutbox = new LinkedBlockingQueue<FeedMessengerMessage>();
            feedMessenger = new FeedMessenger(messengerOutbox);
            (new Thread(feedMessenger)).start();
        }

        public boolean isRegisteredFeedJob(JobId jobId) {
            return registeredFeedConnections.containsKey(jobId) || registeredFeedIntakeJobs.containsKey(jobId);
        }

        public void registerFeedConnectionJob(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobId jobId,
                JobSpecification jobSpec, Map<String, String> feedPolicy) {
            if (registeredFeedConnections.containsKey(jobId)) {
                throw new IllegalStateException(" Feed already registered ");
            }
            FeedCollectInfo info = new FeedCollectInfo(sourceFeedId, feedConnectionId, jobSpec, jobId, feedPolicy);
            registeredFeedConnections.put(jobId, info);
            feedCollectInfos.put(feedConnectionId, info);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed connection [" + jobId + "]" + " for feed ");
            }
        }

        public void deregisterFeedIntakeJob(FeedId feedId, JobId jobId) {
            if (!registeredFeedIntakeJobs.containsKey(jobId)) {
                throw new IllegalStateException(" Feed Intake already registered ");
            }
            registeredFeedIntakeJobs.remove(jobId);
            feedIntakeInfos.remove(feedId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered feed intake job " + jobId + " for feed " + feedId);
            }
        }

        public void registerFeedIntakeJob(FeedId feedId, JobId jobId, JobSpecification jobSpec) {
            if (registeredFeedIntakeJobs.containsKey(jobId)) {
                throw new IllegalStateException(" Feed Intake already registered ");
            }
            FeedIntakeInfo intakeInfo = new FeedIntakeInfo(feedId, jobSpec, jobId);
            registeredFeedIntakeJobs.put(jobId, intakeInfo);
            feedIntakeInfos.put(feedId, intakeInfo);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake job [" + jobId + "]" + " for feed " + feedId);
            }
        }

        public void deregisterFeed(JobId jobId) {
            FeedCollectInfo feedInfo = registeredFeedConnections.remove(jobId);
            if (feedInfo != null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Deregistered Feed Info :" + feedInfo);
                }
            }
        }

        public void deregisterFeed(FeedInfo feedInfo) {
            JobId jobId = feedInfo.jobId;
            deregisterFeed(jobId);
        }

        public FeedIntakeInfo getFeedIntakeInfo(FeedId feedId) {
            return feedIntakeInfos.get(feedId);
        }

        public synchronized void addSubscriptionRequest(FeedSubscriptionRequest request) {
            List<FeedSubscriptionRequest> subscriptionRequests = subscriptions.get(request.getSourceFeed().getFeedId());
            if (subscriptionRequests == null) {
                subscriptionRequests = new ArrayList<FeedSubscriptionRequest>();
                subscriptions.put(request.getSourceFeed().getFeedId(), subscriptionRequests);
            }
            subscriptionRequests.add(request);
        }

        public synchronized void removeSubscriptionRequest(FeedId sourceFeedId, FeedId subsciberFeedId) {
            List<FeedSubscriptionRequest> subscriptionRequests = subscriptions.get(sourceFeedId);
            if (subscriptionRequests == null) {
                throw new IllegalArgumentException("Invalid attempt to remove non-existing subscription. Source feed "
                        + sourceFeedId + " subscribing feed " + subsciberFeedId);
            }
            FeedSubscriptionRequest requestToBeRemoved = null;
            for (FeedSubscriptionRequest request : subscriptionRequests) {
                if (request.getFeed().getFeedId().equals(subsciberFeedId)) {
                    requestToBeRemoved = request;
                }
            }
            if (requestToBeRemoved != null) {
                subscriptionRequests.remove(requestToBeRemoved);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Cancelled subscription of " + sourceFeedId + " by " + subsciberFeedId);
                }
            } else {
                throw new IllegalArgumentException("Invalid attempt to remove non-existing subscription. Source feed "
                        + sourceFeedId + " subscribing feed " + subsciberFeedId);
            }
        }

        public synchronized List<FeedSubscriptionRequest> getSubscriptionRequest(FeedId sourceFeedId) {
            return subscriptions.get(sourceFeedId);
        }

        @Override
        public void run() {
            Message mesg;
            while (true) {
                try {
                    mesg = inbox.take();
                    FeedInfo feedInfo = registeredFeedConnections.get(mesg.jobId);
                    if (feedInfo == null) {
                        feedInfo = registeredFeedIntakeJobs.get(mesg.jobId);
                    }
                    switch (mesg.messageKind) {
                        case JOB_START:
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Job started for feed info" + feedInfo);
                            }
                            handleFeedJobStartMessage(feedInfo, mesg);
                            break;
                        case JOB_FINISH:
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Job finished for feed info" + feedInfo);
                            }
                            handleFeedJobFinishMessage(feedInfo, mesg);
                            deregisterFeed(mesg.jobId);
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }

        private void handleFeedJobStartMessage(FeedInfo feedInfo, Message message) throws Exception {
            switch (feedInfo.infoType) {
                case COLLECT:
                    handleFeedCollectJobStartMessage((FeedCollectInfo) feedInfo, message);
                    break;
                case INTAKE:
                    handleFeedIntakeJobStartMessage((FeedIntakeInfo) feedInfo, message);
                    break;
            }
        }

        private synchronized void handleFeedIntakeJobStartMessage(FeedIntakeInfo feedIntakeInfo, Message message)
                throws Exception {
            List<OperatorDescriptorId> intakeOperatorIds = new ArrayList<OperatorDescriptorId>();
            Map<OperatorDescriptorId, IOperatorDescriptor> operators = feedIntakeInfo.jobSpec.getOperatorMap();
            for (Entry<OperatorDescriptorId, IOperatorDescriptor> entry : operators.entrySet()) {
                IOperatorDescriptor opDesc = entry.getValue();
                if (opDesc instanceof FeedIntakeOperatorDescriptor) {
                    intakeOperatorIds.add(opDesc.getOperatorId());
                }
            }

            IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
            JobInfo info = hcc.getJobInfo(message.jobId);
            feedIntakeInfo.jobInfo = info;
            for (OperatorDescriptorId intakeOperatorId : intakeOperatorIds) {
                Map<Integer, String> operatorLocations = info.getOperatorLocations().get(intakeOperatorId);
                int nOperatorInstances = operatorLocations.size();
                for (int i = 0; i < nOperatorInstances; i++) {
                    feedIntakeInfo.intakeLocations.add(operatorLocations.get(i));
                }
            }

            feedIntakeInfo.state = FeedInfo.State.ACTIVE;
            List<FeedSubscriptionRequest> subscriptionRequests = getSubscriptionRequest(feedIntakeInfo.feedId);
            if (subscriptionRequests != null && !subscriptionRequests.isEmpty()) {
                for (FeedSubscriptionRequest request : subscriptionRequests) {
                    submitFeedSubscriptionRequest(feedIntakeInfo, request);
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("No subscription request yet for " + feedIntakeInfo.feedId);
                }
            }

        }

        private void submitFeedSubscriptionRequest(FeedIntakeInfo feedIntakeInfo, final FeedSubscriptionRequest request) {
            SubscribeFeedWork work = new SubscribeFeedWork(feedIntakeInfo.intakeLocations, request);
            FeedWorkManager.INSTANCE.submitWork(work, new SubscribeFeedWork.FeedSubscribeWorkEventListener());
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Submitted " + work);
            }
        }

        private void handleFeedCollectJobStartMessage(FeedCollectInfo feedCollectInfo, Message message) {
            JobSpecification jobSpec = feedCollectInfo.jobSpec;

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
                feedCollectInfo.jobInfo = info;
                Map<String, String> feedActivityDetails = new HashMap<String, String>();
                StringBuilder ingestLocs = new StringBuilder();
                for (OperatorDescriptorId collectOpId : collectOperatorIds) {
                    Map<Integer, String> operatorLocations = info.getOperatorLocations().get(collectOpId);
                    int nOperatorInstances = operatorLocations.size();
                    for (int i = 0; i < nOperatorInstances; i++) {
                        feedCollectInfo.collectLocations.add(operatorLocations.get(i));
                    }
                }
                StringBuilder computeLocs = new StringBuilder();
                for (OperatorDescriptorId computeOpId : computeOperatorIds) {
                    Map<Integer, String> operatorLocations = info.getOperatorLocations().get(computeOpId);
                    if (operatorLocations != null) {
                        int nOperatorInstances = operatorLocations.size();
                        for (int i = 0; i < nOperatorInstances; i++) {
                            feedCollectInfo.computeLocations.add(operatorLocations.get(i));
                        }
                    } else {
                        feedCollectInfo.computeLocations.addAll(feedCollectInfo.collectLocations);
                    }
                }
                StringBuilder storageLocs = new StringBuilder();
                for (OperatorDescriptorId storageOpId : storageOperatorIds) {
                    Map<Integer, String> operatorLocations = info.getOperatorLocations().get(storageOpId);
                    int nOperatorInstances = operatorLocations.size();
                    for (int i = 0; i < nOperatorInstances; i++) {
                        feedCollectInfo.storageLocations.add(operatorLocations.get(i));
                    }
                }

                ingestLocs.append(StringUtils.join(feedCollectInfo.collectLocations, ","));
                computeLocs.append(StringUtils.join(feedCollectInfo.computeLocations, ","));
                storageLocs.append(StringUtils.join(feedCollectInfo.storageLocations, ","));

                feedActivityDetails.put(FeedActivity.FeedActivityDetails.COLLECT_LOCATIONS, ingestLocs.toString());
                feedActivityDetails.put(FeedActivity.FeedActivityDetails.COMPUTE_LOCATIONS, computeLocs.toString());
                feedActivityDetails.put(FeedActivity.FeedActivityDetails.STORAGE_LOCATIONS, storageLocs.toString());
                feedActivityDetails.put(FeedActivity.FeedActivityDetails.FEED_POLICY_NAME,
                        feedCollectInfo.feedPolicy.get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY));

                FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor(feedCollectInfo.feedPolicy);
                if (policyAccessor.collectStatistics() || policyAccessor.isElastic()) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Feed " + feedCollectInfo.feedConnectionId + " requires Super Feed Manager");
                    }
                    configureSuperFeedManager(feedCollectInfo, feedActivityDetails);
                }

                MetadataManager.INSTANCE.acquireWriteLatch();
                MetadataTransactionContext mdTxnCtx = null;
                try {
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    FeedActivityType nextState = FeedActivityType.FEED_BEGIN;
                    FeedActivity feedActivity = new FeedActivity(feedCollectInfo.feedConnectionId.getFeedId()
                            .getDataverse(), feedCollectInfo.feedConnectionId.getFeedId().getFeedName(),
                            feedCollectInfo.feedConnectionId.getDatasetName(), nextState, feedActivityDetails);
                    MetadataManager.INSTANCE.registerFeedActivity(mdTxnCtx, feedCollectInfo.feedConnectionId,
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

        private void configureSuperFeedManager(FeedCollectInfo feedCollectInfo, Map<String, String> feedActivityDetails) {
            int superFeedManagerIndex = new Random().nextInt(feedCollectInfo.collectLocations.size());
            String superFeedManagerHost = feedCollectInfo.collectLocations.get(superFeedManagerIndex);

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

            feedActivityDetails.put(FeedActivity.FeedActivityDetails.SUPER_FEED_MANAGER_HOST, hostIp);
            feedActivityDetails
                    .put(FeedActivity.FeedActivityDetails.SUPER_FEED_MANAGER_PORT, "" + superFeedManagerPort);

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Super Feed Manager for " + feedCollectInfo.feedConnectionId + " is " + hostIp + " node "
                        + superFeedManagerHost);
            }

            FeedManagerElectMessage feedMessage = new FeedManagerElectMessage(hostIp, superFeedManagerHost,
                    superFeedManagerPort, feedCollectInfo.feedConnectionId);
            superFeedManagerPort += SuperFeedManager.PORT_RANGE_ASSIGNED;
            messengerOutbox.add(new FeedMessengerMessage(feedMessage, feedCollectInfo));
        }

        private void handleFeedJobFinishMessage(FeedInfo feedInfo, Message message) {
            switch (feedInfo.infoType) {
                case COLLECT:
                    handleFeedCollectJobFinishMessage((FeedCollectInfo) feedInfo, message);
                    break;
                case INTAKE:
                    handleFeedIntakeJobFinishMessage((FeedIntakeInfo) feedInfo, message);
                    break;
            }
        }

        private void handleFeedIntakeJobFinishMessage(FeedIntakeInfo feedInfo, Message message) {
            boolean feedFailedDueToPostSubmissionNodeLoss = failedDueToNodeFalilurePostSubmission(feedInfo);
            deregisterFeedIntakeJob(feedInfo.feedId, feedInfo.jobId);
        }

        private void handleFeedCollectJobFinishMessage(FeedCollectInfo feedInfo, Message message) {
            MetadataTransactionContext mdTxnCtx = null;
            boolean latchAcquired = false;
            boolean feedFailedDueToPostSubmissionNodeLoss = failedDueToNodeFalilurePostSubmission(feedInfo);
            if (!feedFailedDueToPostSubmissionNodeLoss) {
                try {
                    IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
                    JobInfo info = hcc.getJobInfo(message.jobId);
                    JobStatus status = info.getPendingStatus();
                    List<Exception> exceptions;
                    boolean failure = status != null && status.equals(JobStatus.FAILURE);
                    FeedActivityType activityType = FeedActivityType.FEED_END;
                    Map<String, String> details = new HashMap<String, String>();
                    if (failure) {
                        exceptions = info.getPendingExceptions();
                        activityType = FeedActivityType.FEED_FAILURE;
                        details.put(FeedActivity.FeedActivityDetails.EXCEPTION_MESSAGE, exceptions.get(0).getMessage());
                        if(LOGGER.isLoggable(Level.INFO)){
                            LOGGER.info(info + " failed on account of " + details);
                        }
                    } else {
                        removeSubscriptionRequest(feedInfo.sourceFeedId, feedInfo.feedConnectionId.getFeedId());
                        if(LOGGER.isLoggable(Level.INFO)){
                            LOGGER.info(info + " completed successfully. Removed subscription");
                        }
                    }
                    mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
                    MetadataManager.INSTANCE.acquireWriteLatch();
                    latchAcquired = true;
                    FeedActivity feedActivity = new FeedActivity(feedInfo.feedConnectionId.getFeedId().getDataverse(),
                            feedInfo.feedConnectionId.getFeedId().getFeedName(),
                            feedInfo.feedConnectionId.getDatasetName(), activityType, details);
                    MetadataManager.INSTANCE.registerFeedActivity(mdTxnCtx, new FeedConnectionId(
                            feedInfo.feedConnectionId.getFeedId().getDataverse(), feedInfo.feedConnectionId.getFeedId()
                                    .getFeedName(), feedInfo.feedConnectionId.getDatasetName()), feedActivity);
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
            } else {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Attempt to revive feed");
                }
                FeedsActivator activator = new FeedsActivator();
                String dataverse = feedInfo.feedConnectionId.getFeedId().getDataverse();
                String datasetName = feedInfo.feedConnectionId.getDatasetName();
                String feedName = feedInfo.feedConnectionId.getFeedId().getFeedName();
                String feedPolicy = feedInfo.feedPolicy.get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY);
                activator.reviveFeed(dataverse, feedName, datasetName, feedPolicy);
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Revived Feed");
                }

            }
        }

        private boolean failedDueToNodeFalilurePostSubmission(FeedInfo feedInfo) {
            JobSpecification spec = feedInfo.jobSpec;
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
            private final FeedInfo feedInfo;

            public FeedMessengerMessage(IFeedMessage message, FeedInfo feedInfo) {
                this.message = message;
                this.feedInfo = feedInfo;
            }

            public IFeedMessage getMessage() {
                return message;
            }

            public FeedInfo getFeedInfo() {
                return feedInfo;
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
                        FeedCollectInfo feedInfo = (FeedCollectInfo) message.getFeedInfo();
                        switch (message.getMessage().getMessageType()) {
                            case SUPER_FEED_MANAGER_ELECT:
                                Thread.sleep(2000);
                                sendSuperFeedManangerElectMessage(feedInfo,
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
    }

    public static class FeedInfo {
        public JobSpecification jobSpec;
        public JobInfo jobInfo;
        public JobId jobId;
        public FeedInfoType infoType;
        public State state;

        public enum State {
            ACTIVE,
            INACTIVE
        }

        public enum FeedInfoType {
            INTAKE,
            COLLECT
        }

        public FeedInfo(JobSpecification jobSpec, JobId jobId, FeedInfoType infoType) {
            this.jobSpec = jobSpec;
            this.jobId = jobId;
            this.infoType = infoType;
            this.state = State.INACTIVE;
        }

        @Override
        public String toString() {
            return " job id " + jobId;
        }
    }

    public static class FeedIntakeInfo extends FeedInfo {
        public List<String> intakeLocations = new ArrayList<String>();
        public FeedId feedId;

        public FeedIntakeInfo(FeedId feedId, JobSpecification jobSpec, JobId jobId) {
            super(jobSpec, jobId, FeedInfoType.INTAKE);
            this.feedId = feedId;
        }

        @Override
        public String toString() {
            return FeedInfoType.INTAKE + "[" + feedId + "]";
        }

    }

    public static class FeedCollectInfo extends FeedInfo {
        public FeedId sourceFeedId;
        public FeedConnectionId feedConnectionId;
        public List<String> collectLocations = new ArrayList<String>();
        public List<String> computeLocations = new ArrayList<String>();
        public List<String> storageLocations = new ArrayList<String>();
        public Map<String, String> feedPolicy;

        public FeedCollectInfo(FeedId sourceFeedId, FeedConnectionId feedConnectionId, JobSpecification jobSpec,
                JobId jobId, Map<String, String> feedPolicy) {
            super(jobSpec, jobId, FeedInfoType.COLLECT);
            this.sourceFeedId = sourceFeedId;
            this.feedConnectionId = feedConnectionId;
            this.feedPolicy = feedPolicy;
        }

        @Override
        public String toString() {
            return FeedInfoType.COLLECT + "[" + feedConnectionId + "]";
        }
    }

    private void insertAffectedRecoverableFeed(String deadNodeId, FeedInfo feedInfo,
            Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> affectedFeeds) {
        Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>> pair = affectedFeeds.get(deadNodeId);
        if (pair == null) {
            List<FeedIntakeInfo> intakeInfos = new ArrayList<FeedIntakeInfo>();
            List<FeedCollectInfo> collectInfos = new ArrayList<FeedCollectInfo>();
            pair = new Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>(intakeInfos, collectInfos);
            affectedFeeds.put(deadNodeId, pair);
        }
        switch (feedInfo.infoType) {
            case INTAKE:
                pair.first.add((FeedIntakeInfo) feedInfo);
                break;
            case COLLECT:
                pair.second.add((FeedCollectInfo) feedInfo);
                break;
        }
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        Set<IClusterManagementWork> workToBeDone = new HashSet<IClusterManagementWork>();
        Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> recoverableFeeds = new HashMap<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>>();
        Collection<FeedCollectInfo> feedCollectInfos = feedJobNotificationHandler.registeredFeedConnections.values();
        Collection<FeedIntakeInfo> feedIntakeInfos = feedJobNotificationHandler.registeredFeedIntakeJobs.values();

        List<FeedCollectInfo> irrecoverableFeeds = new ArrayList<FeedCollectInfo>();
        for (String deadNode : deadNodeIds) {
            for (FeedIntakeInfo feedIntakeInfo : feedIntakeInfos) {
                if (feedIntakeInfo.intakeLocations.contains(deadNode)) {
                    insertAffectedRecoverableFeed(deadNode, feedIntakeInfo, recoverableFeeds);
                }
            }

            for (FeedCollectInfo feedCollectInfo : feedCollectInfos) {
                boolean collectNodeFailure = feedCollectInfo.collectLocations.contains(deadNode);
                boolean computeNodeFailure = feedCollectInfo.computeLocations.contains(deadNode);
                boolean storageNodeFailure = feedCollectInfo.storageLocations.contains(deadNode);
                boolean affectedFeed = collectNodeFailure || computeNodeFailure || storageNodeFailure;
                if (affectedFeed) {
                    reportFeedFailure(feedCollectInfo);
                    boolean recoverableFailure = !storageNodeFailure;
                    if (recoverableFailure) {
                        insertAffectedRecoverableFeed(deadNode, feedCollectInfo, recoverableFeeds);
                    } else {
                        irrecoverableFeeds.add(feedCollectInfo);
                    }
                }
            }
        }

        if (irrecoverableFeeds != null && irrecoverableFeeds.size() > 0) {
            Thread t = new Thread(new FeedsDeActivator(irrecoverableFeeds));
            t.start();
        }

        if (recoverableFeeds.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds.size(), this);
            feedWorkRequestResponseHandler.registerFeedWork(addNodeWork.getWorkId(), recoverableFeeds);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;
    }

    private void reportFeedFailure(FeedCollectInfo failedFeedInfo) {
        FeedActivity fa = null;
        Map<String, String> feedActivityDetails = new HashMap<String, String>();
        MetadataTransactionContext ctx = null;
        MetadataManager.INSTANCE.acquireWriteLatch();
        try {
            ctx = MetadataManager.INSTANCE.beginTransaction();
            fa = new FeedActivity(failedFeedInfo.feedConnectionId.getFeedId().getDataverse(),
                    failedFeedInfo.feedConnectionId.getFeedId().getFeedName(),
                    failedFeedInfo.feedConnectionId.getDatasetName(), FeedActivityType.FEED_FAILURE,
                    feedActivityDetails);
            MetadataManager.INSTANCE.registerFeedActivity(ctx, failedFeedInfo.feedConnectionId, fa);
            MetadataManager.INSTANCE.commitTransaction(ctx);
        } catch (Exception e) {
            if (ctx != null) {
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    throw new IllegalStateException("Unable to abort transaction " + e2);
                }
            }
        } finally {
            MetadataManager.INSTANCE.releaseWriteLatch();
        }
    }

    private static void sendSuperFeedManangerElectMessage(FeedCollectInfo feedInfo, FeedManagerElectMessage electMessage) {
        try {
            Dataverse dataverse = new Dataverse(feedInfo.feedConnectionId.getFeedId().getDataverse(),
                    NonTaggedDataFormat.NON_TAGGED_DATA_FORMAT, 0);
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(dataverse);
            JobSpecification spec = JobSpecificationUtils.createJobSpecification();

            IOperatorDescriptor feedMessenger;
            AlgebricksPartitionConstraint messengerPc;
            Set<String> locations = new HashSet<String>();
            locations.addAll(feedInfo.computeLocations);
            locations.addAll(feedInfo.collectLocations);
            locations.addAll(feedInfo.storageLocations);

            Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> p = metadataProvider.buildSendFeedMessageRuntime(
                    spec, dataverse.getDataverseName(), feedInfo.feedConnectionId.getFeedId().getFeedName(),
                    feedInfo.feedConnectionId.getDatasetName(), electMessage, locations.toArray(new String[] {}));
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
                LOGGER.info("Exception in sending super feed manager elect message: " + feedInfo.feedConnectionId + " "
                        + e.getMessage());
            }
        }
    }

    public static class FeedFailure {

        public enum FailureType {
            COLLECT_NODE,
            COMPUTE_NODE,
            STORAGE_NODE
        }

        public FailureType failureType;
        public String nodeId;

        public FeedFailure(FailureType failureType, String nodeId) {
            this.failureType = failureType;
            this.nodeId = nodeId;
        }

        @Override
        public String toString() {
            return failureType + " (" + nodeId + ") ";
        }
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        State newState = AsterixClusterProperties.INSTANCE.getState();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(joinedNodeId + " joined the cluster. " + "Asterix state: " + newState);
        }

        boolean needToReActivateFeeds = !newState.equals(state) && (newState == State.ACTIVE);
        if (needToReActivateFeeds) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(joinedNodeId + " Resuming loser feeds (if any)");
            }
            try {
                FeedsActivator activator = new FeedsActivator();
                (new Thread(activator)).start();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in resuming feeds" + e.getMessage());
                }
            }
            state = newState;
        } else {
            List<FeedCollectInfo> feedsThatCanBeRevived = new ArrayList<FeedCollectInfo>();
            for (Entry<FeedCollectInfo, List<String>> entry : dependentFeeds.entrySet()) {
                List<String> requiredNodeIds = entry.getValue();
                if (requiredNodeIds.contains(joinedNodeId)) {
                    requiredNodeIds.remove(joinedNodeId);
                    if (requiredNodeIds.isEmpty()) {
                        feedsThatCanBeRevived.add(entry.getKey());
                    }
                }
            }
            if (!feedsThatCanBeRevived.isEmpty()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(joinedNodeId + " Resuming feeds after rejoining of node " + joinedNodeId);
                }
                FeedsActivator activator = new FeedsActivator(feedsThatCanBeRevived);
                (new Thread(activator)).start();
            }
        }
        return null;
    }

    @Override
    public void notifyRequestCompletion(IClusterManagementWorkResponse response) {
        try {
            responseInbox.put(response);
        } catch (InterruptedException e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Interrupted exception");
            }
        }
    }

    @Override
    public void notifyStateChange(State previousState, State newState) {
        switch (newState) {
            case ACTIVE:
                if (previousState.equals(State.UNUSABLE)) {
                    try {
                        FeedsActivator activator = new FeedsActivator();
                        (new Thread(activator)).start();
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Exception in resuming feeds" + e.getMessage());
                        }
                    }
                }
                break;
        }

    }

    private static class FeedsActivator implements Runnable {

        private List<FeedCollectInfo> feedsToRevive;
        private Mode mode;

        public enum Mode {
            REVIVAL_POST_CLUSTER_REBOOT,
            REVIVAL_POST_NODE_REJOIN
        }

        public FeedsActivator() {
            this.mode = Mode.REVIVAL_POST_CLUSTER_REBOOT;
        }

        public FeedsActivator(List<FeedCollectInfo> feedsToRevive) {
            this.feedsToRevive = feedsToRevive;
            this.mode = Mode.REVIVAL_POST_NODE_REJOIN;
        }

        @Override
        public void run() {
            switch (mode) {
                case REVIVAL_POST_CLUSTER_REBOOT:
                    revivePostClusterReboot();
                    break;
                case REVIVAL_POST_NODE_REJOIN:
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e1) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Attempt to resume feed interrupted");
                        }
                        throw new IllegalStateException(e1.getMessage());
                    }
                    for (FeedCollectInfo finfo : feedsToRevive) {
                        try {
                            JobId jobId = AsterixAppContextInfo.getInstance().getHcc().startJob(finfo.jobSpec);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Resumed feed :" + finfo.feedConnectionId + " job id " + jobId);
                                LOGGER.info("Job:" + finfo.jobSpec);
                            }
                        } catch (Exception e) {
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Unable to resume feed " + finfo.feedConnectionId + " " + e.getMessage());
                            }
                        }
                    }
            }
        }

        private void revivePostClusterReboot() {
            MetadataTransactionContext ctx = null;

            try {

                Thread.sleep(4000);
                MetadataManager.INSTANCE.init();
                ctx = MetadataManager.INSTANCE.beginTransaction();
                List<FeedActivity> activeFeeds = MetadataManager.INSTANCE
                        .getActiveFeedsServingADataset(ctx, null, null);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Attempt to resume feeds that were active prior to instance shutdown!");
                    LOGGER.info("Number of feeds affected:" + activeFeeds.size());
                    for (FeedActivity fa : activeFeeds) {
                        LOGGER.info("Active feed " + fa.getDataverseName() + ":" + fa.getDatasetName());
                    }
                }
                for (FeedActivity fa : activeFeeds) {
                    String feedPolicy = fa.getFeedActivityDetails().get(FeedActivityDetails.FEED_POLICY_NAME);
                    FeedPolicy policy = MetadataManager.INSTANCE.getFeedPolicy(ctx, fa.getDataverseName(), feedPolicy);
                    if (policy == null) {
                        policy = MetadataManager.INSTANCE.getFeedPolicy(ctx, MetadataConstants.METADATA_DATAVERSE_NAME,
                                feedPolicy);
                        if (policy == null) {
                            if (LOGGER.isLoggable(Level.SEVERE)) {
                                LOGGER.severe("Unable to resume feed: " + fa.getDataverseName() + ":"
                                        + fa.getDatasetName() + "." + " Unknown policy :" + feedPolicy);
                            }
                            continue;
                        }
                    }

                    FeedPolicyAccessor fpa = new FeedPolicyAccessor(policy.getProperties());
                    if (fpa.autoRestartOnClusterReboot()) {
                        String dataverse = fa.getDataverseName();
                        String datasetName = fa.getDatasetName();
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Resuming feed after cluster revival: " + dataverse + ":" + datasetName
                                    + " using policy " + feedPolicy);
                        }
                        reviveFeed(dataverse, fa.getFeedName(), datasetName, feedPolicy);
                    } else {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Feed " + fa.getDataverseName() + ":" + fa.getDatasetName()
                                    + " governed by policy" + feedPolicy
                                    + " does not state auto restart after cluster revival");
                        }
                    }
                }
                MetadataManager.INSTANCE.commitTransaction(ctx);

            } catch (Exception e) {
                e.printStackTrace();
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e1) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Exception in aborting" + e.getMessage());
                    }
                    throw new IllegalStateException(e1);
                }
            }
        }

        private void reviveFeed(String dataverse, String feedName, String dataset, String feedPolicy) {
            PrintWriter writer = new PrintWriter(System.out, true);
            SessionConfig pc = new SessionConfig(true, false, false, false, false, false, true, true, false);
            try {
                DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(dataverse));
                ConnectFeedStatement stmt = new ConnectFeedStatement(new Identifier(dataverse),
                        new Identifier(feedName), new Identifier(dataset), feedPolicy, 0);
                stmt.setForceConnect(true);
                List<Statement> statements = new ArrayList<Statement>();
                statements.add(dataverseDecl);
                statements.add(stmt);
                AqlTranslator translator = new AqlTranslator(statements, writer, pc, DisplayFormat.TEXT);
                translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, false);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Resumed feed: " + dataverse + ":" + dataset + " using policy " + feedPolicy);
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in resuming loser feed: " + dataverse + ":" + dataset + " using policy "
                            + feedPolicy + " Exception " + e.getMessage());
                }
            }
        }
    }

    public static class FeedsDeActivator implements Runnable {

        private List<FeedCollectInfo> feedsToTerminate;

        public FeedsDeActivator(List<FeedCollectInfo> feedsToTerminate) {
            this.feedsToTerminate = feedsToTerminate;
        }

        @Override
        public void run() {
            for (FeedCollectInfo feedCollectInfo : feedsToTerminate) {
                endFeed(feedCollectInfo);
            }
        }

        private void endFeed(FeedCollectInfo feedInfo) {
            MetadataTransactionContext ctx = null;
            PrintWriter writer = new PrintWriter(System.out, true);
            SessionConfig pc = new SessionConfig(true, false, false, false, false, false, true, true, false);
            try {
                ctx = MetadataManager.INSTANCE.beginTransaction();
                DisconnectFeedStatement stmt = new DisconnectFeedStatement(new Identifier(feedInfo.feedConnectionId
                        .getFeedId().getDataverse()), new Identifier(feedInfo.feedConnectionId.getFeedId()
                        .getFeedName()), new Identifier(feedInfo.feedConnectionId.getDatasetName()));
                List<Statement> statements = new ArrayList<Statement>();
                DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(feedInfo.feedConnectionId.getFeedId()
                        .getDataverse()));
                statements.add(dataverseDecl);
                statements.add(stmt);
                AqlTranslator translator = new AqlTranslator(statements, writer, pc, DisplayFormat.TEXT);
                translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, false);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("End irrecoverable feed: " + feedInfo.feedConnectionId);
                }
                MetadataManager.INSTANCE.commitTransaction(ctx);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in ending loser feed: " + feedInfo.feedConnectionId + " Exception "
                            + e.getMessage());
                }
                e.printStackTrace();
                try {
                    MetadataManager.INSTANCE.abortTransaction(ctx);
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Exception in aborting transaction! System is in inconsistent state");
                    }
                }

            }

        }
    }

    public void submitFeedSubscriptionRequest(FeedSubscriptionRequest subscriptionRequest) {
        synchronized (feedJobNotificationHandler) {
            FeedIntakeInfo intakeInfo = feedJobNotificationHandler.getFeedIntakeInfo(subscriptionRequest
                    .getSourceFeed().getFeedId());
            if (intakeInfo != null) {
                if (intakeInfo.state.equals(FeedInfo.State.ACTIVE)) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("submitting subscription request " + subscriptionRequest);
                    }
                    feedJobNotificationHandler.addSubscriptionRequest(subscriptionRequest);
                    feedJobNotificationHandler.submitFeedSubscriptionRequest(intakeInfo, subscriptionRequest);
                } else {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("adding subscription request " + subscriptionRequest);
                    }
                    feedJobNotificationHandler.addSubscriptionRequest(subscriptionRequest);
                }
            }
        }
    }
}
