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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.api.IAsterixAppRuntimeContext;
import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IFeedAdapter;
import edu.uci.ics.asterix.common.feeds.IFeedIngestionManager;
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
    private final LinkedBlockingQueue<IFeedMessage> inbox;
    private final IFeedIngestionManager feedIngestionManager;
    private IngestionRuntime ingestionRuntime;
    private IFeedAdapter adapter;
    private DistributeFeedFrameWriter feedFrameWriter;

    public FeedIntakeOperatorNodePushable(IHyracksTaskContext ctx, FeedId feedId, IFeedAdapter adapter, int partition,
            IngestionRuntime ingestionRuntime) {
        this.adapter = adapter;
        this.partition = partition;
        this.feedId = feedId;
        this.ingestionRuntime = ingestionRuntime;
        inbox = new LinkedBlockingQueue<IFeedMessage>();
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.feedIngestionManager = runtimeCtx.getFeedManager().getFeedIngestionManager();
    }

    @Override
    public void initialize() throws HyracksDataException {
        IAdapterRuntimeManager adapterExecutor = null;
        FeedPolicyEnforcer policyEnforcer = null;
        try {
            if (ingestionRuntime == null) {
                feedFrameWriter = new DistributeFeedFrameWriter(feedId, writer);
                adapterExecutor = new AdapterRuntimeManager(feedId, adapter, feedFrameWriter, partition, inbox);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Set up feed ingestion activity, would wait for subscribers: " + feedId);
                }
                ingestionRuntime = new IngestionRuntime(feedId, partition, adapterExecutor, feedFrameWriter);
                feedIngestionManager.registerFeedIngestionRuntime(ingestionRuntime);
                feedFrameWriter.open();
                synchronized (adapterExecutor) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Waiting for adaptor [" + partition + "]" + "to be done with ingestion of feed "
                                + feedId);
                    }
                    while (!((AdapterRuntimeManager) adapterExecutor).getState().equals(
                            AdapterRuntimeManager.State.FINISHED_INGESTION)) {
                        adapterExecutor.wait();
                    }
                }
                feedIngestionManager.deregisterFeedIngestionRuntime(ingestionRuntime.getFeedIngestionId());
            } else {
                String message = "Feed Ingestion Runtime for feed " + feedId + " is already registered.";
                LOGGER.severe(message);
                throw new IllegalStateException(message);
            }

            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" Adaptor " + adapter.getClass().getName() + "[" + partition + "]"
                        + " done with ingestion of feed " + feedId);
            }

        } catch (InterruptedException ie) {
            if (policyEnforcer.getFeedPolicyAccessor().continueOnHardwareFailure()) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Continuing on failure as per feed policy, switching to INACTIVE INGESTION temporarily");
                }
                feedFrameWriter.fail();
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Interrupted Exception, something went wrong");
                }

                feedIngestionManager.deregisterFeedIngestionRuntime(ingestionRuntime.getFeedIngestionId());
                feedFrameWriter.close();
                throw new HyracksDataException(ie);
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            feedFrameWriter.close();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Closed Frame Writer " + feedFrameWriter);
            }
        }
    }
}
