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
package org.apache.asterix.external.dataset.adapter;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.external.dataset.adapter.IFeedClient.InflowState;
import org.apache.asterix.metadata.feeds.FeedPolicyEnforcer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;

/**
 * Acts as an abstract class for all pull-based external data adapters. Captures
 * the common logic for obtaining bytes from an external source and packing them
 * into frames as tuples.
 */
public abstract class ClientBasedFeedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(ClientBasedFeedAdapter.class.getName());
    private static final int timeout = 5; // seconds

    protected ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
    protected IFeedClient pullBasedFeedClient;
    protected ARecordType adapterOutputType;
    protected boolean continueIngestion = true;
    protected Map<String, String> configuration;

    private FrameTupleAppender appender;
    private IFrame frame;
    private long tupleCount = 0;
    private final IHyracksTaskContext ctx;
    private int frameTupleCount = 0;

    protected FeedPolicyEnforcer policyEnforcer;

    public FeedPolicyEnforcer getPolicyEnforcer() {
        return policyEnforcer;
    }

    public void setFeedPolicyEnforcer(FeedPolicyEnforcer policyEnforcer) {
        this.policyEnforcer = policyEnforcer;
    }

    public abstract IFeedClient getFeedClient(int partition) throws Exception;

    public abstract ITupleForwardPolicy getTupleParserPolicy();

    public ClientBasedFeedAdapter(Map<String, String> configuration, IHyracksTaskContext ctx) {
        this.ctx = ctx;
        this.configuration = configuration;
    }

    public long getIngestedRecordsCount() {
        return tupleCount;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        appender = new FrameTupleAppender();
        frame = new VSizeFrame(ctx);
        appender.reset(frame, true);
        ITupleForwardPolicy policy = getTupleParserPolicy();
        policy.configure(configuration);
        pullBasedFeedClient = getFeedClient(partition);
        InflowState inflowState = null;
        policy.initialize(ctx, writer);
        while (continueIngestion) {
            tupleBuilder.reset();
            try {
                inflowState = pullBasedFeedClient.nextTuple(tupleBuilder.getDataOutput(), timeout);
                switch (inflowState) {
                    case DATA_AVAILABLE:
                        tupleBuilder.addFieldEndOffset();
                        policy.addTuple(tupleBuilder);
                        frameTupleCount++;
                        break;
                    case NO_MORE_DATA:
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Reached end of feed");
                        }
                        policy.close();
                        tupleCount += frameTupleCount;
                        frameTupleCount = 0;
                        continueIngestion = false;
                        break;
                    case DATA_NOT_AVAILABLE:
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Timed out on obtaining data from pull based adapter. Trying again!");
                        }
                        break;
                }

            } catch (Exception failureException) {
                try {
                    failureException.printStackTrace();
                    boolean continueIngestion = policyEnforcer.continueIngestionPostSoftwareFailure(failureException);
                    if (continueIngestion) {
                        tupleBuilder.reset();
                        continue;
                    } else {
                        throw failureException;
                    }
                } catch (Exception recoveryException) {
                    throw new Exception(recoveryException);
                }
            }
        }
    }

    /**
     * Discontinue the ingestion of data and end the feed.
     * 
     * @throws Exception
     */
    public void stop() throws Exception {
        continueIngestion = false;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

}
