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
import edu.uci.ics.asterix.common.feeds.FeedPointKey;
import edu.uci.ics.asterix.common.feeds.FeedSubscriber;
import edu.uci.ics.asterix.common.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.common.feeds.IFeedLifecycleListener;
import edu.uci.ics.asterix.common.feeds.IFeedPoint;
import edu.uci.ics.asterix.common.feeds.IFeedPoint.Scope;
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
    private final IMessageAnalyzer healthDataParser;
    private final MessageListener healthDataListener;
    private final Map<FeedConnectionId, LinkedBlockingQueue<String>> feedReportQueue;
    private final FeedJobNotificationHandler feedJobNotificationHandler;
    private final FeedWorkRequestResponseHandler feedWorkRequestResponseHandler;
    private final ExecutorService executorService;
    private ClusterState state;

    private FeedLifecycleListener() {
        jobEventInbox = new LinkedBlockingQueue<Message>();
        feedJobNotificationHandler = new FeedJobNotificationHandler(jobEventInbox);
        responseInbox = new LinkedBlockingQueue<IClusterManagementWorkResponse>();
        feedWorkRequestResponseHandler = new FeedWorkRequestResponseHandler(responseInbox, feedJobNotificationHandler);
        feedReportQueue = new HashMap<FeedConnectionId, LinkedBlockingQueue<String>>();
        this.healthDataParser = new FeedHealthDataParser();
        this.healthDataListener = new MessageListener(FEED_HEALTH_PORT, healthDataParser.getMessageQueue());
        this.healthDataListener.start();
        this.executorService = Executors.newCachedThreadPool();
        this.executorService.execute(feedJobNotificationHandler);
        this.executorService.execute(feedWorkRequestResponseHandler);
        ClusterManager.INSTANCE.registerSubscriber(this);
        state = AsterixClusterProperties.INSTANCE.getState();
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

    public static class FeedFailureReport {
        public Map<FeedCollectInfo, List<FeedFailure>> failures = new HashMap<FeedCollectInfo, List<FeedFailure>>();

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<FeedCollectInfo, List<FeedLifecycleListener.FeedFailure>> entry : failures.entrySet()) {
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

    private void insertAffectedRecoverableFeed(String deadNodeId, FeedSubscriber subscriber,
            Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> affectedFeeds) {
        Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>> pair = affectedFeeds.get(deadNodeId);
        if (pair == null) {
            List<FeedIntakeInfo> intakeInfos = new ArrayList<FeedIntakeInfo>();
            List<FeedCollectInfo> collectInfos = new ArrayList<FeedCollectInfo>();
            pair = new Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>(intakeInfos, collectInfos);
            affectedFeeds.put(deadNodeId, pair);
        }
        /*
                switch (feedInfo.infoType) {
                    case INTAKE:
                        pair.first.add((FeedIntakeInfo) feedInfo);
                        break;
                    case COLLECT:
                        pair.second.add((FeedCollectInfo) feedInfo);
                        break;
                }
         */
    }

    @Override
    public Set<IClusterManagementWork> notifyNodeFailure(Set<String> deadNodeIds) {
        Set<IClusterManagementWork> workToBeDone = new HashSet<IClusterManagementWork>();
        Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> recoverableFeeds = new HashMap<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>>();
        Collection<FeedSubscriber> feedSubscribers = feedJobNotificationHandler.getSubscribers();
        Collection<IFeedPoint> feedIntakePoints = feedJobNotificationHandler.getFeedIntakePoints();

        List<FeedSubscriber> irrecoverableFeeds = new ArrayList<FeedSubscriber>();
        for (String deadNode : deadNodeIds) {
            for (IFeedPoint fp : feedIntakePoints) {
                if (fp.getLocations().contains(deadNode)) {
                    //insertAffectedRecoverableFeed(deadNode, fp, recoverableFeeds);
                }
            }

            for (FeedSubscriber subscriber : feedSubscribers) {
                boolean collectNodeFailure = subscriber.getFeedConnectionInfo().getCollectLocations()
                        .contains(deadNode);
                boolean computeNodeFailure = subscriber.getFeedConnectionInfo().getComputeLocations()
                        .contains(deadNode);
                boolean storageNodeFailure = subscriber.getFeedConnectionInfo().getStorageLocations()
                        .contains(deadNode);
                boolean affectedFeed = collectNodeFailure || computeNodeFailure || storageNodeFailure;
                if (affectedFeed) {
                    boolean recoverableFailure = !storageNodeFailure;
                    if (recoverableFailure) {
                        insertAffectedRecoverableFeed(deadNode, subscriber, recoverableFeeds);
                    } else {
                        irrecoverableFeeds.add(subscriber);
                    }
                }
            }
        }

        if (irrecoverableFeeds != null && irrecoverableFeeds.size() > 0) {
            //   Thread t = new Thread(new FeedsDeActivator(irrecoverableFeeds));
            // t.start();
        }

        if (recoverableFeeds.size() > 0) {
            AddNodeWork addNodeWork = new AddNodeWork(deadNodeIds.size(), this);
            feedWorkRequestResponseHandler.registerFeedWork(addNodeWork.getWorkId(), recoverableFeeds);
            workToBeDone.add(addNodeWork);
        }
        return workToBeDone;
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

    public void submitFeedSubscriptionRequest(IFeedPoint feedPoint, FeedSubscriptionRequest subscriptionRequest)
            throws Exception {
        synchronized (feedJobNotificationHandler) {
            feedJobNotificationHandler.submitFeedSubscriptionRequest(feedPoint, subscriptionRequest);
        }
    }

    @Override
    public List<FeedConnectionId> getActiveFeedConnections(FeedId feedId) {
        Collection<FeedSubscriber> subscribers = feedJobNotificationHandler.getSubscribers();
        List<FeedConnectionId> connections = new ArrayList<FeedConnectionId>();
        boolean filter = feedId != null;
        for (FeedSubscriber subscriber : subscribers) {
            if (!filter || filter && subscriber.getFeedConnectionId().getFeedId().equals(feedId)
                    && subscriber.getStatus().equals(FeedSubscriber.Status.ACTIVE)) {
                connections.add(subscriber.getFeedConnectionId());
            }
        }
        return connections;
    }

    @Override
    public List<String> getComputeLocations(FeedId feedId) {
        return feedJobNotificationHandler.getFeedComputeLocations(feedId);
    }

    @Override
    public String[] getIntakeLocations(FeedId feedId) {
        return null;
    }

    @Override
    public String[] getStoreLocations(FeedConnectionId feedConnectionId) {
        return null;
    }

    @Override
    public boolean isFeedActive(FeedId feedId) {
        return false;
    }

    @Override
    public boolean isFeedConnectionActive(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getSourceFeedPoint(connectionId) != null;
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
    public IFeedPoint getAvailableFeedPoint(FeedPointKey feedPointKey) {
        if (isFeedPointAvailable(feedPointKey)) {
            return feedJobNotificationHandler.getFeedPoint(feedPointKey);
        } else {
            return feedJobNotificationHandler.getAvailableFeedPoint(feedPointKey);
        }
    }

    @Override
    public boolean isFeedPointAvailable(FeedPointKey feedPointKey) {
        return feedJobNotificationHandler.isFeedPointAvailable(feedPointKey);
    }

    public void registerFeedPoint(IFeedPoint feedPoint) {
        feedJobNotificationHandler.registerFeedPoint(feedPoint);
    }

    public IFeedPoint getFeedPoint(FeedPointKey feedPointKey) {
        return feedJobNotificationHandler.getFeedPoint(feedPointKey);
    }

    @Override
    public IFeedPoint getSourceFeedPoint(FeedConnectionId connectionId) {
        return feedJobNotificationHandler.getSourceFeedPoint(connectionId);
    }

    public IFeedPoint getFeedPoint(FeedId feedId, Scope scope) {
        return feedJobNotificationHandler.getFeedPoint(feedId, scope);
    }
}
