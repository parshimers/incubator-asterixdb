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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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

    /** A unique identifier for the feed that is operational. **/
    private final FeedId feedId;

    /** Provides mechanism for distributing a frame to multiple readers, each operating in isolation. **/
    private final FrameDistributor frameDistributor;

    /** A map storing the registered frame readers ({@code FrameReader}. **/
    private final Map<IFeedFrameWriter, FeedFrameCollector> registeredCollectors;

    /** The original frame writer instantiated as part of job creation. **/
    private IFrameWriter writer;

    private final FeedRuntimeType feedRuntimeType;

    private final RecordDescriptor recordDescriptor;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public enum DistributionMode {
        SINGLE,
        SHARED,
        INACTIVE
    }

    public DistributeFeedFrameWriter(FeedId feedId, IFrameWriter writer, FeedRuntimeType feedRuntimeType,
            RecordDescriptor recordDescriptor) {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor(feedId);
        this.registeredCollectors = new HashMap<IFeedFrameWriter, FeedFrameCollector>();
        this.feedRuntimeType = feedRuntimeType;
        this.writer = writer;
        this.recordDescriptor = recordDescriptor;
    }

    public synchronized FeedFrameCollector subscribeFeed(IFeedFrameWriter recipientFeedFrameWriter) throws Exception {
        FeedFrameCollector collector = null;
        if (frameDistributor.isRegistered(recipientFeedFrameWriter)) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("subscriber " + recipientFeedFrameWriter.getFeedId() + " already registered");
            }
            collector = registeredCollectors.get(recipientFeedFrameWriter);
        } else {
            LinkedBlockingQueue<DataBucket> inputQueue = new LinkedBlockingQueue<DataBucket>();
            collector = new FeedFrameCollector(inputQueue, recipientFeedFrameWriter);
            registeredCollectors.put(recipientFeedFrameWriter, collector);
            frameDistributor.registerFrameCollector(collector);
            if (frameDistributor.getMode().equals(DistributionMode.SINGLE)) {
                executor.execute(frameDistributor);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Started Frame distributor for " + feedId + " [" + feedRuntimeType + "]");
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Registered subscriber, new mode " + frameDistributor.getMode());
                }
            }
        }
        return collector;
    }

    public synchronized void unsubscribeFeed(IFeedFrameWriter recipientFeedFrameWriter) throws Exception {
        FeedFrameCollector reader = registeredCollectors.get(recipientFeedFrameWriter);
        if (reader != null) {
            frameDistributor.deregisterFrameCollector(reader);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("De-registered frame reader " + reader);
            }
            registeredCollectors.remove(recipientFeedFrameWriter);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed frame writer " + recipientFeedFrameWriter + " is not registered");
            }
        }

    }

    public void notifyEndOfFeed() {
        frameDistributor.notifyEndOfFeed();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
        switch (frameDistributor.mode) {
            case INACTIVE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("FrameDistributor is already in " + frameDistributor.mode);
                }
                break;
            case SINGLE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Will disconnect the lone frame reader in " + frameDistributor.mode + " mode");
                }
                registeredCollectors.values().iterator().next().disconnect();
                break;
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Signalling End Of Feed; currently operating in " + frameDistributor.mode + " mode");
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

    public static class DataBucketPool {
        private final List<DataBucket> pool;

        public DataBucketPool(int size) {
            pool = new ArrayList<DataBucket>();
            for (int i = 0; i < size; i++) {
                DataBucket bucket = new DataBucket(this);
                pool.add(bucket);
            }
        }

        public void returnDataBucket(DataBucket bucket) {
            pool.add(bucket);
        }

        public DataBucket getDataBucket() {
            if (pool.size() == 0) {
                int sleepCycle = 0;
                while (pool.size() != 0 && sleepCycle < 5) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Interrupted" + e);
                        }
                    }
                    sleepCycle++;
                }
                if (sleepCycle == 5) {
                    expandPool(pool.size() / 2);
                }

            }
            return pool.remove(0);
        }

        private void expandPool(int size) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Exppanding Data Bucket pool by " + size + " buckets");
            }
            for (int i = 0; i < size; i++) {
                DataBucket bucket = new DataBucket(this);
                pool.add(bucket);
            }
        }
    }

    public static class DataBucket {

        private static AtomicInteger globalBucketId = new AtomicInteger(0);
        private final ByteBuffer buffer;
        private final AtomicInteger readCount;
        private final DataBucketPool pool;
        private int desiredReadCount;
        private ContentType contentType;
        private AtomicInteger bucketId = new AtomicInteger(0);

        public enum ContentType {
            DATA, // data
            EOD // end of data
        }

        public DataBucket(DataBucketPool pool) {
            buffer = ByteBuffer.allocate(32768);
            readCount = new AtomicInteger(0);
            this.pool = pool;
            this.contentType = ContentType.DATA;
            bucketId.set(globalBucketId.incrementAndGet());
        }

        public void reset(ByteBuffer frame) {
            buffer.flip();
            System.arraycopy(frame.array(), 0, buffer.array(), 0, frame.limit());
            buffer.limit(frame.limit());
            buffer.position(0);
        }

        public synchronized void doneReading() {
            if (readCount.incrementAndGet() == desiredReadCount) {
                readCount.set(0);
                pool.returnDataBucket(this);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("returned data bucket " + this + " back to the pool");
                }
            }
        }

        public void setDesiredReadCount(int rCount) {
            this.desiredReadCount = rCount;
        }

        public ContentType getContentType() {
            return contentType;
        }

        public void setContentType(ContentType contentType) {
            this.contentType = contentType;
        }

        @Override
        public String toString() {
            return "DataBucket [" + bucketId + "]" + " (" + readCount + "," + desiredReadCount + ")";
        }

    }

    private static class FrameDistributor implements Runnable {

        private final FeedId feedId;
        private final LinkedBlockingQueue<DataBucket> inputDataQueue;
        private final List<FeedFrameCollector> pendingAdditions;
        private final List<FeedFrameCollector> pendingDeletions;
        private final List<FeedFrameCollector> registeredCollectors;
        private final DataBucketPool pool;
        private DistributionMode mode;
        private final ExecutorService executor;
        private final int THREAD_POOL_SIZE = 25;

        public FrameDistributor(FeedId feedId) {
            this.feedId = feedId;
            this.pool = new DataBucketPool(THREAD_POOL_SIZE);
            inputDataQueue = new LinkedBlockingQueue<DataBucket>();
            pendingAdditions = new ArrayList<FeedFrameCollector>();
            pendingDeletions = new ArrayList<FeedFrameCollector>();
            this.registeredCollectors = new ArrayList<FeedFrameCollector>();
            mode = DistributionMode.INACTIVE;
            this.executor = Executors.newCachedThreadPool();
        }

        public void notifyEndOfFeed() {
            DataBucket bucket = getDataBucket();
            bucket.setContentType(DataBucket.ContentType.EOD);
            inputDataQueue.add(bucket);
        }

        public synchronized void registerFrameCollector(FeedFrameCollector frameReader) {
            DistributionMode currentMode = mode;
            switch (mode) {
                case INACTIVE:
                    registeredCollectors.add(frameReader);
                    setMode(DistributionMode.SINGLE);
                    break;
                case SINGLE:
                    registeredCollectors.add(frameReader);
                    for (FeedFrameCollector reader : registeredCollectors) {
                        executor.execute(reader);
                    }
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("STARTED Frame Readers in SHARED Mode");
                    }
                    setMode(DistributionMode.SHARED);
                    break;
                case SHARED:
                    executor.execute(frameReader);
                    pendingAdditions.add(frameReader);
                    break;
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Switching to " + mode + " mode from " + currentMode + " mode " + " Feed id " + feedId);
            }
        }

        public synchronized void deregisterFrameCollector(FeedFrameCollector frameCollector) {
            switch (mode) {
                case INACTIVE:
                    throw new IllegalStateException("Invalid attempt to deregister frame collector in " + mode
                            + " mode.");
                case SHARED:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(frameCollector + " marked for removal");
                    }
                    pendingDeletions.add(frameCollector);
                    break;
                case SINGLE:
                    pendingDeletions.add(frameCollector);
                    setMode(DistributionMode.INACTIVE);
                    break;

            }
            frameCollector.setContinueReading(false);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered frame reader" + frameCollector + " from feed distributor for " + feedId);
            }
        }

        private synchronized void setMode(DistributionMode mode) {
            this.mode = mode;
        }

        public boolean isRegistered(IFeedFrameWriter writer) {
            return registeredCollectors.contains(writer);
        }

        public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
            switch (mode) {
                case INACTIVE:
                    if (registeredCollectors != null && !registeredCollectors.isEmpty()) {
                        registeredCollectors.clear();
                    }
                    break;
                case SINGLE:
                    switch (registeredCollectors.get(0).getState()) {
                        case ACTIVE:
                            registeredCollectors.get(0).nextFrame(frame);
                            break;
                        case TRANSITION:
                            DataBucket bucket = getDataBucket();
                            bucket.reset(frame);
                            registeredCollectors.get(0).inputQueue.add(bucket);
                            break;
                        case FINISHED:
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Discarding fetched tuples as feed has ended");
                            }
                            registeredCollectors.remove(0);
                            break;
                    }
                    break;
                case SHARED:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Processing frame in " + DistributionMode.SHARED
                                + " mode. # of registered readers " + registeredCollectors.size());
                    }
                    DataBucket bucket = pool.getDataBucket();
                    bucket.setDesiredReadCount(registeredCollectors.size());
                    bucket.reset(frame);
                    inputDataQueue.add(bucket);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(" Deposited frame " + frame + " for processing by readers ");
                    }
                    break;
            }
        }

        private DataBucket getDataBucket() {
            DataBucket bucket = pool.getDataBucket();
            bucket.setDesiredReadCount(registeredCollectors.size());
            return bucket;
        }

        public void run() {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Starting frame reader " + this + " in distributed mode");
            }
            while (!mode.equals(DistributionMode.INACTIVE)) {
                DataBucket dataBucket;
                try {
                    dataBucket = inputDataQueue.take();
                    synchronized (this) {
                        for (FeedFrameCollector collector : registeredCollectors) {
                            collector.inputQueue.put(dataBucket);
                        }

                        if (pendingAdditions.size() > 0) {
                            registeredCollectors.addAll(pendingAdditions);
                            pendingAdditions.clear();
                        }
                        if (pendingDeletions.size() > 0) {
                            registeredCollectors.removeAll(pendingDeletions);
                            pendingDeletions.clear();
                            if (registeredCollectors.size() == 1) {
                                FeedFrameCollector loneReader = registeredCollectors.get(0);
                                setMode(DistributionMode.SINGLE);
                                loneReader.setState(FeedFrameCollector.State.TRANSITION);
                                DataBucket bucket = getDataBucket();
                                bucket.setContentType(DataBucket.ContentType.EOD);
                                loneReader.inputQueue.add(bucket);
                            } else if (registeredCollectors.size() == 0) {
                                mode = DistributionMode.INACTIVE;
                                if (LOGGER.isLoggable(Level.INFO)) {
                                    LOGGER.info("Distribution is " + DistributionMode.INACTIVE);
                                }
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Finished frame reader " + this + " in distributed mode");
            }
        }

        public DistributionMode getMode() {
            return mode;
        }

    }

    public static class FeedFrameCollector implements Runnable {

        private final LinkedBlockingQueue<DataBucket> inputQueue;
        private IFeedFrameWriter frameWriter;
        private State state;
        private boolean continueReading;

        public enum State {
            ACTIVE,
            FINISHED,
            TRANSITION
        }

        public FeedFrameCollector(LinkedBlockingQueue<DataBucket> inputQueue, IFeedFrameWriter frameWriter) {
            this.inputQueue = inputQueue;
            this.frameWriter = frameWriter;
            this.state = State.ACTIVE;
            this.continueReading = true;
        }

        public void run() {
            while (continueReading) {
                DataBucket dataBucket = null;
                try {
                    dataBucket = inputQueue.take();
                    processDataBucket(dataBucket);
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(this + " processed " + dataBucket);
                    }
                } catch (InterruptedException e) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Interrupted while processing data bucket " + dataBucket);
                    }
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Unable to process data bucket " + dataBucket + ", encountered exception "
                                + e.getMessage());
                    }
                } finally {
                    dataBucket.doneReading();
                }
            }
            if (state.equals(State.TRANSITION)) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(this + " in " + State.TRANSITION + " mode ");
                }
                while (!inputQueue.isEmpty()) {
                    try {
                        processDataBucket(inputQueue.take());
                    } catch (HyracksDataException | InterruptedException e) {
                        e.printStackTrace();
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Exception " + e + " in processing data in " + state);
                        }
                        break;
                    }
                }
                setContinueReading(true);
                setState(State.ACTIVE);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(this + " has transitioned to " + State.ACTIVE + " mode");
                }

            } else {
                disconnect();
            }
        }

        private void processDataBucket(DataBucket bucket) throws HyracksDataException {
            switch (bucket.getContentType()) {
                case DATA:
                    frameWriter.nextFrame(bucket.buffer);
                    break;
                case EOD:
                    continueReading = false;
                    break;
            }
        }

        public synchronized void disconnect() {
            setState(State.FINISHED);
            notifyAll();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Disconnected feed frame collector for " + frameWriter.getFeedId());
            }
        }

        public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
            if (continueReading) {
                frameWriter.nextFrame(frame);
            } else {
                if (state.equals(State.ACTIVE)) {
                    disconnect();
                }
            }
        }

        public synchronized State getState() {
            return state;
        }

        public synchronized void setState(State state) {
            this.state = state;
        }

        public synchronized void setContinueReading(boolean continueReading) {
            this.continueReading = continueReading;
            if (!continueReading && state.equals(State.ACTIVE)) {
                disconnect();
            }
        }

        public IFeedFrameWriter getFrameWriter() {
            return frameWriter;
        }

        public void setFrameWriter(IFeedFrameWriter frameWriter) {
            this.frameWriter = frameWriter;
        }

        @Override
        public String toString() {
            return "FrameCollector [" + frameWriter.getFeedId() + "," + state + "]";
        }

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
