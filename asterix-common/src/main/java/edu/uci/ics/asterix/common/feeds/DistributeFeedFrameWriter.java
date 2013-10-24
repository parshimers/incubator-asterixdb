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
        if (registeredReaders.keySet().contains(recipientFeedFrameWriter)) {
            frameDistributor.deregisterFrameReader(registeredReaders.get(recipientFeedFrameWriter));
            FrameReader reader = registeredReaders.get(recipientFeedFrameWriter);
            reader.setContinueReading(false);
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
                break;
            case SINGLE:
                registeredReaders.values().iterator().next().disconnect();
                break;
            case SHARED:
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

        private final ByteBuffer buffer;
        private final AtomicInteger readCount;
        private final DataBucketPool pool;
        private int desiredReadCount;
        private ContentType contentType;

        public enum ContentType {
            DATA, // data
            EOD // end of data
        }

        public DataBucket(DataBucketPool pool) {
            buffer = ByteBuffer.allocate(32768);
            readCount = new AtomicInteger(0);
            this.pool = pool;
            this.contentType = ContentType.DATA;
        }

        public void reset(ByteBuffer frame) {
            buffer.flip();
            System.arraycopy(frame.array(), 0, buffer.array(), 0, frame.limit());
            buffer.limit(frame.limit());
            buffer.position(0);
        }

        public void doneReading() {
            int activeCount = readCount.incrementAndGet();
            if (activeCount == desiredReadCount) {
                pool.returnDataBucket(this);
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

    }

    private static class FrameDistributor implements Runnable {

        private final AtomicInteger nSubscribers;
        private final LinkedBlockingQueue<DataBucket> inputDataQueue;
        private final List<FrameReader> pendingAdditions;
        private final List<FrameReader> pendingDeletions;
        private final List<FrameReader> registeredReaders;
        private final DataBucketPool pool;
        private DistributionMode mode;

        public FrameDistributor() {
            this.pool = new DataBucketPool(25);
            inputDataQueue = new LinkedBlockingQueue<DataBucket>();
            pendingAdditions = new ArrayList<FrameReader>();
            pendingDeletions = new ArrayList<FrameReader>();
            nSubscribers = new AtomicInteger(0);
            this.registeredReaders = new ArrayList<FrameReader>();
            mode = DistributionMode.INACTIVE;
        }

        public void notifyEndOfFeed() {
            DataBucket bucket = pool.getDataBucket();
            bucket.setContentType(DataBucket.ContentType.EOD);
            inputDataQueue.add(bucket);
        }

        public synchronized void registerFrameReader(FrameReader frameReader) {
            pendingAdditions.add(frameReader);
            registeredReaders.add(frameReader);
            if (registeredReaders.size() == 1) {
                mode = DistributionMode.SINGLE;
            } else if (registeredReaders.size() > 1) {
                mode = DistributionMode.SHARED;
            }
        }

        public synchronized void deregisterFrameReader(FrameReader frameReader) {
            pendingDeletions.add(frameReader);
            registeredReaders.remove(frameReader);
            if (registeredReaders.size() == 1) {
                mode = DistributionMode.SINGLE;
            } else if (registeredReaders.size() == 0) {
                mode = DistributionMode.INACTIVE;
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
                    registeredReaders.get(0).nextFrame(frame);
                    break;
                case SHARED:
                    DataBucket bucket = pool.getDataBucket();
                    bucket.reset(frame);
                    inputDataQueue.add(bucket);
                    break;
            }
        }

        public void run() {
            while (true) {
                DataBucket dataBucket;
                try {
                    dataBucket = inputDataQueue.take();
                    for (FrameReader reader : registeredReaders) {
                        reader.inputQueue.put(dataBucket);
                    }
                    synchronized (this) {
                        if (pendingAdditions.size() > 0) {
                            registeredReaders.addAll(pendingAdditions);
                            nSubscribers.set(nSubscribers.get() + pendingAdditions.size());
                            pendingAdditions.clear();
                        }
                        if (pendingDeletions.size() > 0) {
                            registeredReaders.removeAll(pendingAdditions);
                            nSubscribers.set(nSubscribers.get() - pendingDeletions.size());
                            pendingDeletions.clear();
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public DistributionMode getMode() {
            return mode;
        }

    }

    public static class FrameReader implements Runnable {

        private final LinkedBlockingQueue<DataBucket> inputQueue;
        private final IFeedFrameWriter frameWriter;
        private State state;
        private boolean continueReading;

        public enum State {
            ACTIVE,
            FINISHED
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
                    switch (data.getContentType()) {
                        case DATA:
                            frameWriter.nextFrame(data.buffer);
                            data.doneReading();
                            break;
                        case EOD:
                            continueReading = false;
                            break;
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (HyracksDataException e) {
                    e.printStackTrace();
                } finally {
                    data.doneReading();
                }
            }
            disconnect();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Disconnected frame reader for " + frameWriter.getFeedId());
            }
        }

        public synchronized void disconnect() {
            state = State.FINISHED;
            notifyAll();
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

        public State getState() {
            return state;
        }

        public synchronized void setContinueReading(boolean continueReading) {
            this.continueReading = continueReading;
            if (!continueReading && state.equals(State.ACTIVE)) {
                disconnect();
            }
        }
    }

}
