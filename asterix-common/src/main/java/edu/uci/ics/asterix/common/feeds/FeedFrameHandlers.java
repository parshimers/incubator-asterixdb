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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FrameDistributor.RoutingMode;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class FeedFrameHandlers {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameHandlers.class.getName());

    public static IFeedFrameHandler getFeedFrameHandler(FrameDistributor distributor, FeedId feedId,
            RoutingMode routingMode, FeedRuntimeType runtimeType, int partition) throws IOException {
        IFeedFrameHandler handler = null;
        switch (routingMode) {
            case IN_MEMORY_ROUTE:
                handler = new InMemoryRouter(distributor.getRegisteredReaders().values(), runtimeType, partition);
                break;
            case SPILL_TO_DISK:
                handler = new DiskSpiller(distributor, feedId, runtimeType, partition);
                break;
            case DISCARD:
                handler = new DiscardRouter(distributor, feedId, runtimeType, partition);
                break;
            default:
                throw new IllegalArgumentException("Invalid routing mode" + routingMode);
        }
        return handler;
    }

    public static class DiscardRouter implements IFeedFrameHandler {

        private final FeedId feedId;
        private int nDiscarded;
        private final FeedRuntimeType runtimeType;
        private final int partition;
        private final FrameDistributor distributor;

        public DiscardRouter(FrameDistributor distributor, FeedId feedId, FeedRuntimeType runtimeType, int partition)
                throws HyracksDataException {
            this.distributor = distributor;
            this.feedId = feedId;
            this.nDiscarded = 0;
            this.runtimeType = runtimeType;
            this.partition = partition;
        }

        @Override
        public void handleFrame(ByteBuffer frame) throws HyracksDataException {
            FrameTupleAccessor fta = distributor.getFta();
            fta.reset(frame);
            int nTuples = fta.getTupleCount();
            nDiscarded += nTuples;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Discarded additional [" + runtimeType + "]" + "(" + partition + ")" + "  " + nTuples);
            }
        }

        @Override
        public void handleDataBucket(DataBucket bucket) {
            nDiscarded++;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Discard Count" + nDiscarded);
            }
        }

        @Override
        public void close() {
            // do nothing, no resource to relinquish
        }

        @Override
        public Iterator<ByteBuffer> replayData() throws Exception {
            throw new IllegalStateException("Invalid operation");
        }

        @Override
        public String toString() {
            return "DiscardRouter" + "[" + feedId + "]" + "(" + nDiscarded + ")";
        }

        @Override
        public String getSummary() {
            return new String("Number of discarded frames (since last reset)" + " feedId " + "[" + feedId + "]" + "("
                    + nDiscarded + ")");
        }

    }

    public static class InMemoryRouter implements IFeedFrameHandler {

        private final Collection<FeedFrameCollector> frameCollectors;

        public InMemoryRouter(Collection<FeedFrameCollector> frameCollectors, FeedRuntimeType runtimeType, int partition) {
            this.frameCollectors = frameCollectors;
        }

        @Override
        public void handleFrame(ByteBuffer frame) throws HyracksDataException {
            throw new IllegalStateException("Operation not supported");
        }

        @Override
        public void handleDataBucket(DataBucket bucket) {
            for (FeedFrameCollector collector : frameCollectors) {
                collector.sendMessage(bucket);
            }
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public Iterator<ByteBuffer> replayData() throws Exception {
            throw new IllegalStateException("Operation not supported");
        }

        @Override
        public String getSummary() {
            return "InMemoryRouter Summary";
        }
    }

    public static class DiskSpiller implements IFeedFrameHandler {

        private final FeedId feedId;
        private FrameSpiller<ByteBuffer> receiver;
        private Iterator<ByteBuffer> iterator;

        public DiskSpiller(FrameDistributor distributor, FeedId feedId, FeedRuntimeType runtimeType, int partition)
                throws IOException {
            this.feedId = feedId;
            receiver = new FrameSpiller<ByteBuffer>(distributor, feedId);
        }

        @Override
        public void handleFrame(ByteBuffer frame) throws HyracksDataException {
            receiver.sendMessage(frame);
        }

        private static class FrameSpiller<T> extends MessageReceiver<ByteBuffer> {

            private final BufferedOutputStream bos;
            private final ByteBuffer reusableLengthBuffer;
            private final ByteBuffer reusableDataBuffer;
            private long offset;

            private final File file;

            public FrameSpiller(FrameDistributor distributor, FeedId feedId) throws IOException {
                Date date = new Date();
                String dateSuffix = date.toString().replace(' ', '_');
                String filename = feedId.toString() + "_" + distributor.getFeedRuntimeType() + "_"
                        + distributor.getPartition() + "_" + dateSuffix;
                file = new File(filename);
                if (!file.exists()) {
                    boolean success = file.createNewFile();
                    if (!success) {
                        throw new IOException("Unable to create spill file for feed " + feedId);
                    }
                }
                bos = new BufferedOutputStream(new FileOutputStream(file));
                reusableLengthBuffer = ByteBuffer.allocate(4);
                reusableDataBuffer = ByteBuffer.allocate(32768);
                this.offset = 0;
            }

            @Override
            public void processMessage(ByteBuffer message) throws Exception {
                reusableLengthBuffer.flip();
                reusableLengthBuffer.putInt(message.array().length);
                bos.write(reusableLengthBuffer.array());
                bos.write(message.array());
            }

            @SuppressWarnings("resource")
            public Iterator<ByteBuffer> replayData() throws Exception {
                final BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
                bis.skip(offset);
                return new Iterator<ByteBuffer>() {

                    @Override
                    public boolean hasNext() {
                        boolean more = false;
                        try {
                            more = bis.available() > 0;
                            if (!more) {
                                bis.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return more;
                    }

                    @Override
                    public ByteBuffer next() {
                        reusableLengthBuffer.flip();
                        try {
                            bis.read(reusableLengthBuffer.array());
                            reusableLengthBuffer.flip();
                            int frameSize = reusableLengthBuffer.getInt();
                            reusableDataBuffer.flip();
                            bis.read(reusableDataBuffer.array(), 0, frameSize);
                            offset += 4 + frameSize;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return reusableDataBuffer;
                    }

                    @Override
                    public void remove() {
                    }

                };
            }

        }

        @Override
        public void handleDataBucket(DataBucket bucket) {
            throw new IllegalStateException("Operation not supported");
        }

        @Override
        public void close() {
            receiver.close(true);
        }

        @Override
        public Iterator<ByteBuffer> replayData() throws Exception {
            iterator = receiver.replayData();
            return iterator;
        }

        @Override
        public String getSummary() {
            return "Disk Spiller Summary";
        }

    }

    public static class FeedMemoryEventListener implements IMemoryEventListener {

        private static final Logger LOGGER = Logger.getLogger(FeedMemoryEventListener.class.getName());
        private FrameDistributor frameDistributor;

        public FeedMemoryEventListener(FrameDistributor frameDistributor) {
            this.frameDistributor = frameDistributor;
        }

        @Override
        public void processEvent(MemoryEventType eventType) {
            switch (eventType) {
                case MEMORY_AVAILABLE:
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("Memomry event of type " + eventType + " notification");
                    }
                    switch (frameDistributor.getRoutingMode()) {
                        case SPILL_TO_DISK:
                            try {
                                Iterator<ByteBuffer> replayIterator = frameDistributor.getDiskSpillHandler()
                                        .replayData();
                                while (replayIterator.hasNext()) {
                                    ByteBuffer buffer = replayIterator.next();
                                    frameDistributor.handleInMemoryRouteMode(buffer);
                                }
                            } catch (Exception e) {
                                if (LOGGER.isLoggable(Level.WARNING)) {
                                    LOGGER.warning("Unable to process spilled frames for feed "
                                            + frameDistributor.getFeedId());
                                }
                            }
                            break;
                        case DISCARD:
                            frameDistributor.setRoutingMode(RoutingMode.IN_MEMORY_ROUTE);
                            break;
                        case IN_MEMORY_ROUTE:
                            // nothing to do 
                            break;
                    }
                    break;
            }
        }
    }

}
