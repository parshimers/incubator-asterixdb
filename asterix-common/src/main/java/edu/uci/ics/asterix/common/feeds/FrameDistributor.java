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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.DistributionMode;
import edu.uci.ics.asterix.common.feeds.IFeedMemoryComponent.Type;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameDistributor {

    private static final Logger LOGGER = Logger.getLogger(FrameDistributor.class.getName());

    private final FeedId feedId;
    private final List<FeedFrameCollector> registeredCollectors;
    private DataBucketPool pool;
    private DistributionMode mode;
    private final IFeedMemoryManager memoryManager;

    public FrameDistributor(FeedId feedId, IFeedMemoryManager memoryManager) {
        this.feedId = feedId;
        this.memoryManager = memoryManager;
        this.registeredCollectors = new ArrayList<FeedFrameCollector>();
        mode = DistributionMode.INACTIVE;
    }

    public void notifyEndOfFeed() {
        DataBucket bucket = getDataBucket();
        bucket.setContentType(DataBucket.ContentType.EOD);
        processMessage(bucket);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("End of feed signal received");
        }
    }

    public synchronized void registerFrameCollector(FeedFrameCollector frameCollector) {
        DistributionMode currentMode = mode;
        switch (mode) {
            case INACTIVE:
                registeredCollectors.add(frameCollector);
                setMode(DistributionMode.SINGLE);
                break;
            case SINGLE:
                pool = (DataBucketPool) memoryManager.getMemoryComponent(Type.POOL);
                registeredCollectors.add(frameCollector);
                for (FeedFrameCollector reader : registeredCollectors) {
                    reader.start();
                }
                setMode(DistributionMode.SHARED);
                break;
            case SHARED:
                frameCollector.start();
                registeredCollectors.add(frameCollector);
                break;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to " + mode + " mode from " + currentMode + " mode " + " Feed id " + feedId);
        }
    }

    public synchronized void deregisterFrameCollector(FeedFrameCollector frameCollector) {
        switch (mode) {
            case INACTIVE:
                throw new IllegalStateException("Invalid attempt to deregister frame collector in " + mode + " mode.");
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closing collector " + frameCollector);
                }
                frameCollector.closeCollector();
                registeredCollectors.remove(frameCollector);
                int nCollectors = registeredCollectors.size();
                if (nCollectors == 1) {
                    FeedFrameCollector loneCollector = registeredCollectors.get(0);
                    setMode(DistributionMode.SINGLE);
                    loneCollector.setState(FeedFrameCollector.State.TRANSITION);
                    loneCollector.closeCollector();
                    memoryManager.releaseMemoryComponent(pool);
                }
                break;
            case SINGLE:
                frameCollector.closeCollector();
                setMode(DistributionMode.INACTIVE);
                break;

        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Deregistered frame reader" + frameCollector + " from feed distributor for " + feedId);
        }
    }

    public synchronized void setMode(DistributionMode mode) {
        this.mode = mode;
    }

    public boolean isRegistered(IFeedFrameWriter writer) {
        return registeredCollectors.contains(writer);
    }

    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        switch (mode) {
            case INACTIVE:
                break;
            case SINGLE:
                FeedFrameCollector collector = registeredCollectors.get(0);
                switch (collector.getState()) {
                    case ACTIVE:
                        collector.nextFrame(frame); //processing is synchronous
                        break;
                    case TRANSITION:
                        DataBucket bucket = getDataBucket();
                        bucket.reset(frame);
                        collector.sendMessage(bucket); //processing is asynchronous
                        break;
                    case FINISHED:
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Discarding fetched tuples as feed has ended ["
                                    + registeredCollectors.get(0) + "]" + " Feed Id " + feedId);
                        }
                        registeredCollectors.remove(0);
                        break;
                }
                break;
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Processing frame in " + DistributionMode.SHARED + " mode. # of registered readers "
                            + registeredCollectors.size() + " feedId " + feedId);
                }
                DataBucket bucket = pool.getDataBucket();
                bucket.setDesiredReadCount(registeredCollectors.size());
                bucket.reset(frame);
                processMessage(bucket);
                break;
        }
    }

    private synchronized void processMessage(DataBucket bucket) {
        for (FeedFrameCollector collector : registeredCollectors) {
            collector.sendMessage(bucket); //processing is asynchronous
        }
    }

    private DataBucket getDataBucket() {
        DataBucket bucket = pool.getDataBucket();
        bucket.setDesiredReadCount(registeredCollectors.size());
        return bucket;
    }

    public DistributionMode getMode() {
        return mode;
    }

}