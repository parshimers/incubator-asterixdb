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
package edu.uci.ics.asterix.api.common;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.ConnectFeedStatement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.SubscribeFeedStatement;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.common.feeds.IFeedWork;
import edu.uci.ics.asterix.common.feeds.IFeedWorkEventListener;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedCollectInfo;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.entities.FeedActivity;
import edu.uci.ics.asterix.metadata.entities.FeedActivity.FeedActivityDetails;
import edu.uci.ics.asterix.metadata.entities.FeedPolicy;
import edu.uci.ics.asterix.metadata.feeds.FeedPolicyAccessor;
import edu.uci.ics.asterix.metadata.feeds.FeedSubscriptionRequest;
import edu.uci.ics.asterix.metadata.feeds.FeedSubscriptionRequest.SubscriptionStatus;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.job.JobId;

/**
 * A collection of feed management related task, each represented as an implementation of {@code IFeedWork}.
 */
public class FeedWorkCollection {

    private static Logger LOGGER = Logger.getLogger(FeedWorkCollection.class.getName());

    /**
     * The task of subscribing to a feed to obtain data.
     */
    public static class SubscribeFeedWork implements IFeedWork {

        private final Runnable runnable;

        private final FeedSubscriptionRequest request;

        private final List<String> locations;

        @Override
        public Runnable getRunnable() {
            return runnable;
        }

        public SubscribeFeedWork(List<String> locations, FeedSubscriptionRequest request) {
            this.runnable = new SubscribeFeedWorkRunnable(locations, request);
            this.request = request;
            this.locations = locations;
        }

        private static class SubscribeFeedWorkRunnable implements Runnable {

            private final FeedSubscriptionRequest request;
            private final List<String> locations;

            public SubscribeFeedWorkRunnable(List<String> locations, FeedSubscriptionRequest request) {
                this.request = request;
                this.locations = locations;
            }

            @Override
            public void run() {
                try {
                    PrintWriter writer = new PrintWriter(System.out, true);
                    SessionConfig pc = new SessionConfig(true, false, false, false, false, false, true, true, false);
                    DataverseDecl dataverseDecl = new DataverseDecl(new Identifier(request.getSourceFeed()
                            .getDataverseName()));
                    SubscribeFeedStatement subscribeStmt = new SubscribeFeedStatement(locations, request);
                    List<Statement> statements = new ArrayList<Statement>();
                    statements.add(dataverseDecl);
                    statements.add(subscribeStmt);
                    AqlTranslator translator = new AqlTranslator(statements, writer, pc, DisplayFormat.TEXT);
                    translator.compileAndExecute(AsterixAppContextInfo.getInstance().getHcc(), null, false);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Submitted subscribed feed stmt for execution: " + request);
                    }
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Exception in executing " + request);
                    }
                    throw new RuntimeException(e);
                }
            }
        }

        public static class FeedSubscribeWorkEventListener implements IFeedWorkEventListener {

            @Override
            public void workFailed(IFeedWork work, Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning(" Feed subscription request " + ((SubscribeFeedWork) work).request
                            + " failed with exception " + e);
                }
            }

            @Override
            public void workCompleted(IFeedWork work) {
                ((SubscribeFeedWork) work).request.setSubscriptionStatus(SubscriptionStatus.ACTIVE);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.warning(" Feed subscription request " + ((SubscribeFeedWork) work).request + " completed ");
                }
            }

        }

        public FeedSubscriptionRequest getRequest() {
            return request;
        }

        @Override
        public String toString() {
            return "SubscribeFeedWork for [" + request + "]";
        }

    }

    /**
     * The task of activating a set of feeds.
     */
    public static class ActivateFeedWork implements IFeedWork {

        private final Runnable runnable;

        @Override
        public Runnable getRunnable() {
            return runnable;
        }

        public ActivateFeedWork(List<FeedCollectInfo> feedsToRevive) {
            this.runnable = new FeedsActivateRunnable(feedsToRevive);
        }

        public ActivateFeedWork() {
            this.runnable = new FeedsActivateRunnable();
        }

        private static class FeedsActivateRunnable implements Runnable {

            private List<FeedCollectInfo> feedsToRevive;
            private Mode mode;

            public enum Mode {
                REVIVAL_POST_CLUSTER_REBOOT,
                REVIVAL_POST_NODE_REJOIN
            }

            public FeedsActivateRunnable(List<FeedCollectInfo> feedsToRevive) {
                this.feedsToRevive = feedsToRevive;
            }

            public FeedsActivateRunnable() {
                this.mode = Mode.REVIVAL_POST_CLUSTER_REBOOT;
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
                                    LOGGER.warning("Unable to resume feed " + finfo.feedConnectionId + " "
                                            + e.getMessage());
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
                    List<FeedActivity> activeFeeds = MetadataManager.INSTANCE.getActiveFeedsServingADataset(ctx, null,
                            null);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Attempt to resume feeds that were active prior to instance shutdown!");
                        LOGGER.info("Number of feeds affected:" + activeFeeds.size());
                        for (FeedActivity fa : activeFeeds) {
                            LOGGER.info("Active feed " + fa.getDataverseName() + ":" + fa.getDatasetName());
                        }
                    }
                    for (FeedActivity fa : activeFeeds) {
                        String feedPolicy = fa.getFeedActivityDetails().get(FeedActivityDetails.FEED_POLICY_NAME);
                        FeedPolicy policy = MetadataManager.INSTANCE.getFeedPolicy(ctx, fa.getDataverseName(),
                                feedPolicy);
                        if (policy == null) {
                            policy = MetadataManager.INSTANCE.getFeedPolicy(ctx,
                                    MetadataConstants.METADATA_DATAVERSE_NAME, feedPolicy);
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
                    ConnectFeedStatement stmt = new ConnectFeedStatement(new Identifier(dataverse), new Identifier(
                            feedName), new Identifier(dataset), feedPolicy, 0);
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
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Exception in resuming loser feed: " + dataverse + ":" + dataset + " using policy "
                                + feedPolicy + " Exception " + e.getMessage());
                    }
                }
            }
        }

    }
}
