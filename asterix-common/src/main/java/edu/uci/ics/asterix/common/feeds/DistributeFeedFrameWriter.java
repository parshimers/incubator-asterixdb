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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
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
    private final Map<IFeedFrameWriter, FrameReader> registeredReaders;

    /** The original frame writer instantiated as part of job creation. **/
    private IFrameWriter writer;

    public enum DistributionMode {
        SINGLE,
        SHARED,
        INACTIVE
    }

    public DistributeFeedFrameWriter(FeedId feedId, IFrameWriter writer) {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor();
        this.registeredReaders = new HashMap<IFeedFrameWriter, FrameReader>();
        this.writer = writer;
    }

    public synchronized FrameReader subscribeFeed(IFeedFrameWriter recipientFeedFrameWriter) throws Exception {
        FrameReader reader = null;
        if (frameDistributor.isRegistered(recipientFeedFrameWriter)) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("subscriber " + recipientFeedFrameWriter.getFeedId() + " already registered");
            }
            reader = registeredReaders.get(recipientFeedFrameWriter);
        } else {
            LinkedBlockingQueue<DataBucket> outputQueue = new LinkedBlockingQueue<DataBucket>();
            reader = new FrameReader(outputQueue, recipientFeedFrameWriter);
            registeredReaders.put(recipientFeedFrameWriter, reader);
            frameDistributor.registerFrameReader(reader);
            if (frameDistributor.getMode().equals(DistributionMode.SINGLE)) {
                setUpDistribution();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Started feed ingestion on registering subscriber ");
                }
            }
        }
        return reader;
    }

    public synchronized void unsubscribeFeed(IFeedFrameWriter recipientFeedFrameWriter) throws Exception {
        FrameReader reader = registeredReaders.get(recipientFeedFrameWriter);
        if (reader != null) {
            frameDistributor.deregisterFrameReader(reader);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("De-registered frame reader " + reader);
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed frame writer " + recipientFeedFrameWriter + " is not registered");
            }
        }

    }

    private void setUpDistribution() throws Exception {
        (new Thread(frameDistributor)).start();
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
                registeredReaders.values().iterator().next().disconnect();
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
            return pool.get(0);
        }

        private void expandPool(int size) {
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

        public void doneReading() {
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
            return "DataBucket [" + bucketId + "]" + " (" + readCount + "," + desiredReadCount + " )";
        }

    }

    private static class FrameDistributor implements Runnable {

        private final LinkedBlockingQueue<DataBucket> inputDataQueue;
        private final List<FrameReader> pendingAdditions;
        private final List<FrameReader> pendingDeletions;
        private final List<FrameReader> registeredReaders;
        private final DataBucketPool pool;
        private DistributionMode mode;
        private final ExecutorService executor;
        private final int THREAD_POOL_SIZE = 25;

        public FrameDistributor() {
            this.pool = new DataBucketPool(THREAD_POOL_SIZE);
            inputDataQueue = new LinkedBlockingQueue<DataBucket>();
            pendingAdditions = new ArrayList<FrameReader>();
            pendingDeletions = new ArrayList<FrameReader>();
            this.registeredReaders = new ArrayList<FrameReader>();
            mode = DistributionMode.INACTIVE;
            this.executor = Executors.newCachedThreadPool();
        }

        public void notifyEndOfFeed() {
            DataBucket bucket = getDataBucket();
            bucket.setContentType(DataBucket.ContentType.EOD);
            inputDataQueue.add(bucket);
        }

        public synchronized void registerFrameReader(FrameReader frameReader) {
            DistributionMode currentMode = mode;
            switch (mode) {
                case INACTIVE:
                    registeredReaders.add(frameReader);
                    mode = DistributionMode.SINGLE;
                    break;
                case SINGLE:
                    registeredReaders.add(frameReader);
                    mode = DistributionMode.SHARED;
                    for (FrameReader reader : registeredReaders) {
                        executor.execute(reader);
                    }
                    break;
                case SHARED:
                    executor.execute(frameReader);
                    pendingAdditions.add(frameReader);
                    break;
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Switching to " + mode + " mode from " + currentMode + " mode");
            }
        }

        public synchronized void deregisterFrameReader(FrameReader frameReader) {
            switch (mode) {
                case INACTIVE:
                    throw new IllegalStateException("Invalid attempt to deregister frame reader in " + mode + " mode.");
                case SHARED:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info(frameReader + " marked for removal");
                    }
                    pendingDeletions.add(frameReader);
                    break;
                case SINGLE:
                    pendingDeletions.add(frameReader);
                    mode = DistributionMode.INACTIVE;
                    break;

            }
            frameReader.setContinueReading(false);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered frame reader" + frameReader);
            }
        }

        public boolean isRegistered(IFeedFrameWriter writer) {
            return registeredReaders.contains(writer);
        }

        public void nextFrame(ByteBuffer frame) throws HyracksDataException {
            switch (mode) {
                case INACTIVE:
                    break;
                case SINGLE:
                    switch (registeredReaders.get(0).getState()) {
                        case ACTIVE:
                            registeredReaders.get(0).nextFrame(frame);
                            break;
                        case TRANSITION:
                            DataBucket bucket = getDataBucket();
                            bucket.reset(frame);
                            registeredReaders.get(0).inputQueue.add(bucket);
                            break;
                        case FINISHED:
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Discarding fetched tuples as feed has ended");
                            }
                    }
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Single Frame Reader " + registeredReaders.get(0));
                    }
                    break;
                case SHARED:
                    DataBucket bucket = pool.getDataBucket();
                    bucket.setDesiredReadCount(registeredReaders.size());
                    bucket.reset(frame);
                    inputDataQueue.add(bucket);
                    break;
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Frame  processed " + frame + " processed in mode [" + mode + "]");
            }
        }

        private DataBucket getDataBucket() {
            DataBucket bucket = pool.getDataBucket();
            bucket.setDesiredReadCount(registeredReaders.size());
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
                        for (FrameReader reader : registeredReaders) {
                            reader.inputQueue.put(dataBucket);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info(reader + " processedd " + dataBucket);
                            }
                        }
                        if (pendingAdditions.size() > 0) {
                            registeredReaders.addAll(pendingAdditions);
                            pendingAdditions.clear();
                        }
                        if (pendingDeletions.size() > 0) {
                            registeredReaders.removeAll(pendingDeletions);
                            pendingDeletions.clear();
                            if (registeredReaders.size() == 1) {
                                FrameReader loneReader = registeredReaders.get(0);
                                mode = DistributionMode.SINGLE;
                                loneReader.setState(FrameReader.State.TRANSITION);
                                DataBucket bucket = getDataBucket();
                                bucket.setContentType(DataBucket.ContentType.EOD);
                                loneReader.inputQueue.add(bucket);
                            } else if (registeredReaders.size() == 0) {
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

    public static class FrameReader implements Runnable {

        private final LinkedBlockingQueue<DataBucket> inputQueue;
        private IFeedFrameWriter frameWriter;
        private State state;
        private boolean continueReading;

        public enum State {
            ACTIVE,
            FINISHED,
            TRANSITION
        }

        public FrameReader(LinkedBlockingQueue<DataBucket> inputQueue, IFeedFrameWriter frameWriter) {
            this.inputQueue = inputQueue;
            this.frameWriter = frameWriter;
            this.state = State.ACTIVE;
            this.continueReading = true;
        }

        public void run() {
            while (continueReading) {
                DataBucket data = null;
                try {
                    data = inputQueue.take();
                    processDataBucket(data);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                } finally {
                    data.doneReading();
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
                LOGGER.info("Disconnected frame reader for " + frameWriter.getFeedId());
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
            return "FrameReader [" + frameWriter.getFeedId() + "," + state + "]";
        }

    }

}
