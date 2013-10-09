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

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IAdapterRuntimeManager;
import edu.uci.ics.asterix.common.feeds.IFeedFrameWriter;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DistributeFeedFrameWriter implements IFeedFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(DistributeFeedFrameWriter.class.getName());

    private final FeedId feedId;

    private final FrameDistributor frameDistributor;

    private IAdapterRuntimeManager adapterRuntimeManager;

    private final Map<IFeedFrameWriter, FrameReader> registeredReaders;

    private IFrameWriter writer;

    public DistributeFeedFrameWriter(FeedId feedId, IFrameWriter writer) {
        this.feedId = feedId;
        this.frameDistributor = new FrameDistributor();
        this.registeredReaders = new HashMap<IFeedFrameWriter, FrameReader>();
        this.writer = writer;
    }

    public void setAdapterRuntimeManager(IAdapterRuntimeManager adapterRuntimeManager) {
        this.adapterRuntimeManager = adapterRuntimeManager;
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
            if (frameDistributor.getMode().equals(FrameDistributor.DistributionMode.SINGLE)) {
                beginIngestion();
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
            if (frameDistributor.getMode().equals(FrameDistributor.DistributionMode.INACTIVE)) {
                endIngestion();
            }
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Feed frame writer " + recipientFeedFrameWriter + " is not registerd");
            }
        }
    }

    private void beginIngestion() throws Exception {
        (new Thread(frameDistributor)).start();
        adapterRuntimeManager.start();
    }

    private void endIngestion() throws Exception {
        adapterRuntimeManager.stop();
    }

    private void notifyEndOfFeed() {
        frameDistributor.notifyEndOfFeed();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
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

        public enum DistributionMode {
            SINGLE,
            SHARED,
            INACTIVE
        }

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
                    registeredReaders.get(0).getFrameWriter().nextFrame(frame);
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

        public enum State {
            ACTIVE,
            FINISHED
        }

        public FrameReader(LinkedBlockingQueue<DataBucket> inputQueue, IFeedFrameWriter frameWriter) {
            this.inputQueue = inputQueue;
            this.frameWriter = frameWriter;
            this.state = State.ACTIVE;
        }

        public void run() {
            while (true) {
                DataBucket data = null;
                try {
                    data = inputQueue.take();
                    switch (data.getContentType()) {
                        case DATA:
                            frameWriter.nextFrame(data.buffer);
                            data.doneReading();
                            break;
                        case EOD:
                            state = State.FINISHED;
                            synchronized (this) {
                                notifyAll();
                            }
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
        }

        public IFeedFrameWriter getFrameWriter() {
            return frameWriter;
        }

        public State getState() {
            return state;
        }
    }

}
