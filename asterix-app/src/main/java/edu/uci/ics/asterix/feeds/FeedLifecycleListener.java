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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DisconnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.api.IClusterManagementWork;
import edu.uci.ics.asterix.common.api.IClusterManagementWork.ClusterState;
import edu.uci.ics.asterix.common.api.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedJointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.IFeedJoint;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleEventSubscriber;
import edu.uci.ics.asterix.common.feeds.IFeedJoint.Scope;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.ClusterManager;
import edu.uci.ics.asterix.metadata.feeds.BuiltinFeedPolicies;
import edu.uci.ics.asterix.metadata.feeds.FeedCollectOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.FeedIntakeOperatorDescriptor;
import edu.uci.ics.asterix.metadata.feeds.MessageListener;
import edu.uci.ics.asterix.metadata.feeds.MessageListener.IMessageAnalyzer;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

/**
 * A listener that subscribes to events associated with cluster membership
 * (nodes joining/leaving the cluster) and job lifecycle (start/end of a job).
 * Subscription to such events allows keeping track of feed ingestion jobs and
 * take any corrective action that may be required when a node involved in a
 * feed leaves the cluster.
 */
public class FeedLifecycleListener implements IFeedLifecycleListener {

    private static final Logger LOGGER = Logger.getLogger(FeedLifecycleListener.class.getName());

    public static FeedLifecycleListener INSTANCE = new FeedLifecycleListener();

    public static final int FEED_HEALTH_PORT = 2999;

    private final LinkedBlockingQueue<Message> jobEventInbox;
    private final LinkedBlockingQueue<IClusterManagementWorkResponse> responseInbox;
    private final Map<FeedCollectInfo, List<String>> dependentFeeds = new HashMap<FeedCollectInfo, List<String>>();
    private final Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final FeedWorkRequestResponseHandler feedWorkRequestResponseHandler;
    private final ExecutorService executorService;
    private ClusterState state;

    private FeedLifecycleListener() {
        this.jobEventInbox = new LinkedBlockingQueue<Message>();
        this.feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        this.responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        this.feedWorkRequestResponseHandler = new FeedWorkRequestResponseHandler(responseInbox,
                feedJobNotificationHandler);
        this.feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(feedJobNotificationHandler);
        this.executorService.execute(feedWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        this.state = AsterixClusterProperties.INSTANCE.getState();
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

    /*
     * Traverse job specification to categorize job as a feed intake job or a feed collection job 
     */
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
                feedPolicy = ((FeedCollectOperatorDescriptor) opDesc).getFeedPolicyProperties();
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
            feedJobNotificationHandler.registerFeedCollectionJob(sourceFeedId, feedConnectionId, jobId, spec,
                    feedPolicy);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed collection: " + feedConnectionId + " ingestion policy "
                        + feedPolicy.get(BuiltinFeedPolicies.CONFIG_FEED_POLICY_KEY));
            }
        } else if (feedIntakeJob) {
            feedJobNotificationHandler.registerFeedIntakeJob(feedId, jobId, spec);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered feed intake job for feed: " + feedId);
            }
        }
    }

    public static class Message {
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

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        Set<IClusterManagementWork> workToBeDone = new HashSet<IClusterManagementWork>();
        Collection<FeedSubscriber> subscribers = feedJobNotificationHandler.getSubscribers();
        List<FeedSubscriber> irrecoverableSubscribers = new ArrayList<FeedSubscriber>();

        List<Pair<FeedSubscriber, List<String>>> recoverableSubscribers = new ArrayList<Pair<FeedSubscriber, List<String>>>();
        for (FeedSubscriber subscriber : subscribers) {
            List<String> deadNodes = new ArrayList<String>();
            boolean isFailureRecoverable = true;
            boolean failure = false;
            Pair<FeedSubscriber, List<String>> p = new Pair<FeedSubscriber, List<String>>(subscriber, deadNodes);
            for (String deadNode : deadNodeIds) {
                boolean failureBecauseOfThisNode = subscriber.getFeedConnectionInfo().getCollectLocations()
                        .contains(deadNode)
                        || subscriber.getFeedConnectionInfo().getComputeLocations().contains(deadNode)
                        || subscriber.getFeedConnectionInfo().getStorageLocations().contains(deadNode);
                if (failureBecauseOfThisNode) {
                    failure = true;
                    isFailureRecoverable = isFailureRecoverable
                            && !subscriber.getFeedConnectionInfo().getStorageLocations().contains(deadNode);
                    if (isFailureRecoverable) {
                        deadNodes.add(deadNode);
                    } else {
                        irrecoverableSubscribers.add(subscriber);
                        break;
                    }
                }
            }
            if (failure && isFailureRecoverable) {
                recoverableSubscribers.add(p);
            }
        }

        Collection<IFeedJoint> intakeFeedJoints = feedJobNotificationHandler.getFeedIntakePoints();
        Map<IFeedJoint, List<String>> recoverableIntakeFeedIds = new HashMap<IFeedJoint, List<String>>();
        for (IFeedJoint feedJoint : intakeFeedJoints) {
            List<String> deadNodes = new ArrayList<String>();
            for (String deadNode : deadNodeIds) {
                if (feedJoint.getLocations().contains(deadNode)) {
                    deadNodes.add(deadNode);
                }
            }
            if (deadNodes.size() > 0) {
                recoverableIntakeFeedIds.put(feedJoint, deadNodes);
            }
        }

        FailureReport failureReport = new FailureReport(recoverableIntakeFeedIds, recoverableSubscribers);

        if (irrecoverableSubscribers.size() > 0) {
            executorService.execute(new FeedsDeActivator(irrecoverableSubscribers));
        }

        if (recoverableSubscribers.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds, deadNodeIds.size(), this);
            feedWorkRequestResponseHandler.registerFeedWork(addNodeWork.getWorkId(), failureReport);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;
    }

    public static class FailureReport {

        private final List<Pair<FeedSubscriber, List<String>>> recoverableSubscribers;
        private final Map<IFeedJoint, List<String>> recoverableIntakeFeedIds;

        public FailureReport(Map<IFeedJoint, List<String>> recoverableIntakeFeedIds,
                List<Pair<FeedSubscriber, List<String>>> recoverableSubscribers) {
            this.recoverableSubscribers = recoverableSubscribers;
            this.recoverableIntakeFeedIds = recoverableIntakeFeedIds;

        }

        public List<Pair<FeedSubscriber, List<String>>> getRecoverableSubscribers() {
            return recoverableSubscribers;
        }

        public Map<IFeedJoint, List<String>> getRecoverableIntakeFeedIds() {
            return recoverableIntakeFeedIds;
        }

    }

    @Override
    public Set<IClusterManagementWork> notifyNodeJoin(String joinedNodeId) {
        ClusterState newState = AsterixClusterProperties.INSTANCE.getState();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(joinedNodeId + " joined the cluster. " + "Asterix state: " + newState);
        }

        boolean needToReActivateFeeds = !newState.equals(state) && (newState == ClusterState.ACTIVE);
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
    public void notifyStateChange(ClusterState previousState, ClusterState newState) {
        switch (newState) {
            case ACTIVE:
                if (previousState.equals(ClusterState.UNUSABLE)) {
                    try {
                        FeedsActivator activator = new FeedsActivator();
                        // (new Thread(activator)).start();
                    } catch (Exception e) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Exception in resuming feeds" + e.getMessage());
                        }
                    }
                }
                break;
        }

    }

    public static class FeedsDeActivator implements Runnable {

        private List<FeedSubscriber> failedSubscribers;

        public FeedsDeActivator(List<FeedSubscriber> failedSubscribers) {
            this.failedSubscribers = failedSubscribers;
        }

        @Override
        public void run() {
            for (FeedSubscriber subscribers : failedSubscribers) {
                endFeed(subscribers);
            }
        }

        private void endFeed(FeedSubscriber subscriber) {
            MetadataTransactionContext ctx = null;
            PrintWriter writer = new PrintWriter(System.out, true);
            SessionConfig pc = new SessionConfig(true, false, false, false, false, false, true, true, false);
            try {
                ctx = MetadataManager.INSTANCE.beginTransaction();
                FeedId feedId = subscriber.getFeedConnectionId().getFeedId();
                DisconnectFeedStatement stmt = new DisconnectFeedStatement(new Identifier(feedId.getDataverse()),
                        new Identifier(feedId.getFeedName()), new Identifier(subscriber.getFeedConnectionId()
                                .getDatasetName()));
                List<Statement> statements = new ArrayList<Statement>();
                DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(feedId.getDataverse()));
                statements.add(dataverseDecl);
                statements.add(stmt);
                AqlTranslator translator = new AqlTranslator(statements, writer, pc, DisplayFormat.TEXT);
                translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, false);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("End irrecoverable feed: " + subscriber.getFeedConnectionId());
                }
                MetadataManager.INSTANCE.commitTransaction(ctx);
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Exception in ending loser feed: " + subscriber.getFeedConnectionId() + " Exception "
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

    public void submitFeedSubscriptionRequest(IFeedJoint feedPoint, FeedSubscriptionRequest subscriptionRequest)
            throws Exception {
        feedJobNotificationHandler.submitFeedSubscriptionRequest(feedPoint, subscriptionRequest);
    }

    @Override
    public List<FeedConnectionId> getActiveFeedConnections(FeedId feedId) {
        List<FeedConnectionId> connections = new ArrayList<FeedConnectionId>();
        Collection<FeedConnectionId> activeConnections = feedJobNotificationHandler.getActiveFeedConnections();
        if (feedId != null) {
            for (FeedConnectionId connectionId : activeConnections) {
                if (connectionId.getFeedId().equals(feedId)) {
                    connections.add(connectionId);
                }
            }
        } else {
            connections.addAll(activeConnections);
        }
        return connections;
    }

    @Override
    public List<String> getComputeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedComputeLocations(feedId);
    }

    @Override
    public String[] getIntakeLocations(FeedId feedId) {
        Collection<IFeedJoint> intakeFeedJoints = feedJobNotificationHandler.getFeedIntakePoints();
        for (IFeedJoint feedJoint : intakeFeedJoints) {
            if (feedJoint.getFeedJointKey().getFeedId().equals(feedId)) {
                return feedJoint.getLocations().toArray(new String[] {});
            }
        }
        return null;
    }

    @Override
    public String[] getStoreLocations(FeedConnectionId feedConnectionId) {
        return feedJobNotificationHandler.getFeedStorageLocations(feedConnectionId).toArray(new String[] {});
    }

    @Override
    public boolean isFeedConnectionActive(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.isFeedConnectionActive(connectionId);
    }

    public void reportPartialDisconnection(FeedConnectionId connectionId) {
        feedJobNotificationHandler.deregisterFeedConnection(connectionId);
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

    @Override
    public IFeedJoint getAvailableFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getAvailableFeedJoint(feedJointKey);
    }

    @Override
    public boolean isFeedJointAvailable(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.isFeedPointAvailable(feedJointKey);
    }

    public void registerFeedJoint(IFeedJoint feedJoint) {
        feedJobNotificationHandler.registerFeedJoint(feedJoint);
    }

    public IFeedJoint getFeedJoint(FeedJointKey feedJointKey) {
        return feedJobNotificationHandler.getFeedJoint(feedJointKey);
    }

    @Override
    public IFeedJoint getSourceFeedJoint(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getSourceFeedPoint(connectionId);
    }

    public IFeedJoint getFeedJoint(FeedId feedId, Scope scope) {
        return feedJobNotificationHandler.getFeedPoint(feedId, scope);
    }

    public void registerFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.registerFeedEventSubscriber(connectionId, subscriber);
    }

    public void deregisterFeedEventSubscriber(FeedConnectionId connectionId, IFeedLifecycleEventSubscriber subscriber) {
        feedJobNotificationHandler.deregisterFeedEventSubscriber(connectionId, subscriber);

    }

}
