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
package edu.uci.ics.asterix.metadata.feeds;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.CollectionRuntime;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager.State;
import edu.uci.ics.asterix.common.feeds.IFeedAdapter;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.asterix.common.feeds.ISubscriberRuntime;
import edu.uci.ics.asterix.common.feeds.IngestionRuntime;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

/**
 * The runtime for @see{FeedIntakeOperationDescriptor}.
 * The core functionality provided by this pushable is to set up the artifacts for ingestion of a feed.
 * The artifacts are lazily activated when a feed receives a subscription request.
 */
public class FeedIntakeOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {

    private static Logger LOGGER = Logger.getLogger(FeedIntakeOperatorNodePushable.class.getName());

    private final int partition;
    private final FeedId feedId;
    private final IFeedSubscriptionManager feedSubscriptionManager;
    private IngestionRuntime ingestionRuntime;
    private IFeedAdapter adapter;
    private DistributeFeedFrameWriter feedFrameWriter;
    private IAdapterFactory adapterFactory;
    private IHyracksTaskContext ctx;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, FeedId feedId, IAdapterFactory adapterFactory,
            int partition, IngestionRuntime ingestionRuntime) {
        this.ctx = ctx;
        this.partition = partition;
        this.feedId = feedId;
        this.ingestionRuntime = ingestionRuntime;
        this.adapterFactory = adapterFactory;
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedSubscriptionManager = runtimeCtx.getFeedManager().getFeedSubscriptionManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        IAdapterRuntimeManager adapterExecutor = null;
        try {
            if (ingestionRuntime == null) {
                try {
                    adapter = (IFeedAdapter) adapterFactory.createAdapter(ctx, partition);
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.SEVERE)) {
                        LOGGER.severe("Unable to create adapter : " + adapterFactory.getName() + "[" + partition + "]"
                                + " Exception " + e);
                    }
                    throw new HyracksDataException(e);
                }
                feedFrameWriter = new DistributeFeedFrameWriter(feedId, writer, FeedRuntimeType.INGEST, recordDesc);
                adapterExecutor = new AdapterRuntimeManager(feedId, adapter, feedFrameWriter, partition);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Set up feed ingestion activity, would wait for subscribers: " + feedId);
                }
                ingestionRuntime = new IngestionRuntime(feedId, partition, adapterExecutor, feedFrameWriter);
                feedSubscriptionManager.registerFeedSubscribableRuntime(ingestionRuntime);
                feedFrameWriter.open();
                waitTillIngestionIsOver(adapterExecutor);
                feedSubscriptionManager.deregisterFeedSubscribableRuntime(ingestionRuntime
                        .getFeedSubscribableRuntimeId());
            } else {
                if (ingestionRuntime.getAdapterRuntimeManager().getState().equals(State.INACTIVE_INGESTION)) {
                    ingestionRuntime.getAdapterRuntimeManager().setState(State.ACTIVE_INGESTION);
                    adapter = ingestionRuntime.getAdapterRuntimeManager().getFeedAdapter();
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(" Switching to " + State.ACTIVE_INGESTION + " for ingestion runtime "
                                + ingestionRuntime);
                        LOGGER.info(" Adaptor " + adapter.getClass().getName() + "[" + partition + "]"
                                + " connected to backend for feed " + feedId);
                    }
                    feedFrameWriter = (DistributeFeedFrameWriter) ingestionRuntime.getFeedFrameWriter();
                    waitTillIngestionIsOver(ingestionRuntime.getAdapterRuntimeManager());
                } else {
                    String message = "Feed Ingestion Runtime for feed " + feedId + " is already registered.";
                    LOGGER.severe(message);
                    throw new IllegalStateException(message);
                }
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" Adaptor " + adapter.getClass().getName() + "[" + partition + "]"
                        + " done with ingestion of feed " + feedId);
            }

        } catch (InterruptedException ie) {
            List<ISubscriberRuntime> subscribers = ingestionRuntime.getSubscribers();
            FeedPolicyAccessor policyAccessor = new FeedPolicyAccessor();
            boolean needToHandleFailure = false;
            List<ISubscriberRuntime> failingSubscribers = new ArrayList<ISubscriberRuntime>();
            for (ISubscriberRuntime subscriber : subscribers) {
                policyAccessor.reset(subscriber.getFeedPolicy());
                if (!policyAccessor.continueOnHardwareFailure()) {
                    failingSubscribers.add(subscriber);
                } else {
                    needToHandleFailure = true;
                }
            }

            for (ISubscriberRuntime failingSubscriber : failingSubscribers) {
                try {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Unsubscribing failing feed (not configured to recover) " + failingSubscriber
                                + " on occurrence of failure. ");
                    }
                    ingestionRuntime.unsubscribeFeed((CollectionRuntime) failingSubscriber);
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Excpetion in unsubscribing " + failingSubscriber + " message " + e.getMessage());
                    }
                }
            }

            if (needToHandleFailure) {
                ingestionRuntime.getAdapterRuntimeManager().setState(State.INACTIVE_INGESTION);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Switching to " + State.INACTIVE_INGESTION + " on occurrence of failure. ");
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Interrupted Exception, something went wrong. None of the subscribers need to handle failures. Shutting down feed ingestion");
                }
                feedSubscriptionManager.deregisterFeedSubscribableRuntime(ingestionRuntime
                        .getFeedSubscribableRuntimeId());
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            if (!ingestionRuntime.getAdapterRuntimeManager().getState().equals(State.INACTIVE_INGESTION)) {
                feedFrameWriter.close();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closed Frame Writer " + feedFrameWriter + " adapter state "
                            + ingestionRuntime.getAdapterRuntimeManager().getState());
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Ending intake operator node pushable in state " + State.INACTIVE_INGESTION
                            + " Will resume after correcting failure");
                }
            }

        }
    }

    private void waitTillIngestionIsOver(IAdapterRuntimeManager adaoterRuntimeManager) throws InterruptedException {
        synchronized (adaoterRuntimeManager) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Waiting for adaptor [" + partition + "]" + "to be done with ingestion of feed " + feedId);
            }
            while (!adaoterRuntimeManager.getState().equals(IAdapterRuntimeManager.State.FINISHED_INGESTION)) {
                adaoterRuntimeManager.wait();
            }
        }
    }
}
