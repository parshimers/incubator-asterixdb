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
package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * Provides mechanism for distributing the frames, as received from an operator to a
 * set of registered readers. Each reader typically operates at a different pace. Readers
 * are isolated from each other to ensure that a slow reader does not impact the progress of
 * others.
 **/
public class DistributeFeedFrameWriter implements IFeedFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(DistributeFeedFrameWriter.class.getName());

    /** A unique identifier for the feed to which the incoming tuples belong. **/
    private final FeedId feedId;

    /** Provides mechanism for distributing a frame to multiple readers, each operating in isolation. **/
    private final FrameDistributor frameDistributor;

    /** A map storing the registered frame readers ({@code FeedFrameCollector}. **/
    private final Map<IFeedFrameWriter, FeedFrameCollector> registeredCollectors;

    /** The original frame writer instantiated as part of job creation. **/
    private IFrameWriter writer;

    /** The feed operation whose output is being distributed by the DistributeFeedFrameWriter. **/
    private final FeedRuntimeType feedRuntimeType;

    /** RecordDescriptor {@code RecordDescriptor} representing the output from the DistributeFeedFrameWriter. **/
    private final RecordDescriptor recordDescriptor;

    public enum DistributionMode {
        /** A single feed frame collector is registered for receiving tuples. **/
        SINGLE,

        /** Multiple feed frame collectors are concurrently registered for receiving tuples. **/
        SHARED,

        /** Feed tuples are not being routed, irrespective of # of registered feed frame collectors. **/
        INACTIVE
    }

    public DistributeFeedFrameWriter(FeedId feedId, IFrameWriter writer, FeedRuntimeType feedRuntimeType,
            RecordDescriptor recordDescriptor, IFeedMemoryManager memoryManager) {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor(feedId, memoryManager);
        this.registeredCollectors = new HashMap<IFeedFrameWriter, FeedFrameCollector>();
        this.feedRuntimeType = feedRuntimeType;
        this.writer = writer;
        this.recordDescriptor = recordDescriptor;
    }

    public synchronized FeedFrameCollector subscribeFeed(IFeedFrameWriter frameWriter) throws Exception {
        FeedFrameCollector collector = null;
        if (!frameDistributor.isRegistered(frameWriter)) {
            collector = new FeedFrameCollector(frameWriter);
            registeredCollectors.put(frameWriter, collector);
            frameDistributor.registerFrameCollector(collector);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered subscriber, new mode " + frameDistributor.getMode());
            }
            return collector;
        } else {
            throw new IllegalStateException("subscriber " + frameWriter.getFeedId() + " already registered");
        }
    }

    public synchronized void unsubscribeFeed(IFeedFrameWriter recipientFeedFrameWriter) throws Exception {
        FeedFrameCollector frameCollector = registeredCollectors.get(recipientFeedFrameWriter);
        if (frameCollector != null) {
            frameDistributor.deregisterFrameCollector(frameCollector);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("De-registered frame reader " + frameCollector);
            }
            registeredCollectors.remove(recipientFeedFrameWriter);
        } else {
            throw new IllegalStateException("Invalid attempt to unregister FeedFrameWriter " + recipientFeedFrameWriter
                    + " not registered.");
        }
    }

    public void notifyEndOfFeed() {
        frameDistributor.notifyEndOfFeed();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
        DistributionMode mode = frameDistributor.getMode();
        switch (frameDistributor.getMode()) {
            case INACTIVE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("FrameDistributor is " + mode);
                }
                break;
            case SINGLE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Will disconnect the lone frame reader in " + mode + " mode " + " FeedId " + feedId);
                }
                frameDistributor.setMode(DistributionMode.INACTIVE);
                registeredCollectors.values().iterator().next().disconnect();
                break;
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Signalling End Of Feed; currently operating in " + mode + " mode");
                }
                notifyEndOfFeed();
                break;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        frameDistributor.nextFrame(frame);
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public FeedId getFeedId() {
        return feedId;
    }

    public DistributionMode getDistributionMode() {
        return frameDistributor.getMode();
    }

    public IFrameWriter getWriter() {
        return writer;
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    public Map<IFeedFrameWriter, FeedFrameCollector> getRegisteredReaders() {
        return registeredCollectors;
    }

    public FeedRuntimeType getFeedRuntimeType() {
        return feedRuntimeType;
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

    @Override
    public Type getType() {
        return IFeedFrameWriter.Type.DISTRIBUTE_FEED_WRITER;
    }

}
