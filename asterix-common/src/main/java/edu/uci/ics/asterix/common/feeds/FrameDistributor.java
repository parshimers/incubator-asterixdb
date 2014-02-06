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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DistributeFeedFrameWriter.DistributionMode;
import edu.uci.ics.asterix.common.feeds.FeedFrameHandlers.FeedMemoryEventListener;
import edu.uci.ics.asterix.common.feeds.IFeedMemoryComponent.Type;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameDistributor {

    private static final Logger LOGGER = Logger.getLogger(FrameDistributor.class.getName());

    private final FeedId feedId;
    private final FeedRuntimeType feedRuntimeType;
    private final int partition;
    private final List<FeedFrameCollector> registeredCollectors;
    private DataBucketPool pool;
    private DistributionMode mode;
    private final IFeedMemoryManager memoryManager;
    private IFeedFrameHandler inMemoryHandler;
    private IFeedFrameHandler diskSpillHandler;
    private RoutingMode routingMode;
    private IMemoryEventListener mListener;

    public static enum RoutingMode {
        IN_MEMORY_ROUTE,
        SPILL_TO_DISK,
        DISCARD
    }

    public FrameDistributor(FeedId feedId, FeedRuntimeType feedRuntimeType, int partition,
            IFeedMemoryManager memoryManager) throws IOException {
        this.feedId = feedId;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.memoryManager = memoryManager;
        this.registeredCollectors = new ArrayList<FeedFrameCollector>();
        mode = DistributionMode.INACTIVE;
        routingMode = RoutingMode.IN_MEMORY_ROUTE;
        inMemoryHandler = FeedFrameHandlers.getFeedFrameHandler(this, feedId, RoutingMode.IN_MEMORY_ROUTE);
        diskSpillHandler = FeedFrameHandlers.getFeedFrameHandler(this, feedId, RoutingMode.SPILL_TO_DISK);
        mListener = new FeedMemoryEventListener(this);
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
        switch (routingMode) {
            case IN_MEMORY_ROUTE:
                handleInMemoryRouteMode(frame);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("In-memory routing of " + feedId + "(" + feedRuntimeType + ")" + "[" + partition + "]");
                }
                break;
            case SPILL_TO_DISK:
                handleSpillToDiskMode(frame);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Spilling frame to disk due to memory pressure " + feedId + "(" + feedRuntimeType + ")"
                            + "[" + partition + "]");
                }
                break;
            case DISCARD:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Discarding frame due to memory pressure " + feedId + "(" + feedRuntimeType + ")" + "["
                            + partition + "]");
                }
                break;
        }
    }

    void handleInMemoryRouteMode(ByteBuffer frame) throws HyracksDataException {
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
                        if (bucket == null) {
                            switchRoutingMode(frame); // memory congestion
                        } else {
                            bucket.reset(frame);
                            inMemoryHandler.handleDataBucket(bucket);
                        }
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
                DataBucket bucket = pool.getDataBucket();
                if (bucket == null) {
                    switchRoutingMode(frame); // memory congestion
                } else {
                    bucket.setDesiredReadCount(registeredCollectors.size());
                    bucket.reset(frame);
                    inMemoryHandler.handleDataBucket(bucket);
                }
                break;
        }

    }

    private void handleSpillToDiskMode(ByteBuffer frame) throws HyracksDataException {
        try {
            diskSpillHandler.handleFrame(frame);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    private void switchRoutingMode(ByteBuffer frame) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Unable to allocate memory, will evaluate the need to spill");
        }
        boolean spillToDisk = false;
        int maxSpillSizeMB = 0;
        for (FeedFrameCollector collector : registeredCollectors) {
            FeedPolicyAccessor fpa = collector.getFeedPolicyAccessor();
            if (fpa.spillToDiskOnCongestion()) {
                spillToDisk = true;
                int mss = fpa.getMaxSpillOnDisk();
                if (maxSpillSizeMB < mss) {
                    maxSpillSizeMB = mss;
                }
            }
        }
        try {
            if (spillToDisk) {
                setRoutingMode(RoutingMode.SPILL_TO_DISK);
                diskSpillHandler.handleFrame(frame);
            } else {
                setRoutingMode(RoutingMode.DISCARD);
            }

            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Switched to " + routingMode + " mode.");
            }

        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    private synchronized void processMessage(DataBucket bucket) {
        for (FeedFrameCollector collector : registeredCollectors) {
            collector.sendMessage(bucket); //processing is asynchronous
        }
    }

    private DataBucket getDataBucket() {
        DataBucket bucket = pool.getDataBucket();
        if (bucket != null) {
            bucket.setDesiredReadCount(registeredCollectors.size());
            return bucket;
        } else {
            return null;
        }
    }

    public DistributionMode getMode() {
        return mode;
    }

    public RoutingMode getRoutingMode() {
        return routingMode;
    }

    public void setRoutingMode(RoutingMode routingMode) {
        switch (routingMode) {
            case DISCARD:
            case SPILL_TO_DISK:
                memoryManager.registerMemoryEventListener(mListener);
                break;
            case IN_MEMORY_ROUTE:
                memoryManager.unregisterMemoryEventListener(mListener);
                break;
        }

        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching from " + this.routingMode + "  to " + routingMode);
        }
        this.routingMode = routingMode;
    }

    public List<FeedFrameCollector> getRegisteredCollectors() {
        return registeredCollectors;
    }

    public IFeedFrameHandler getInMemoryHandler() {
        return inMemoryHandler;
    }

    public FeedId getFeedId() {
        return feedId;
    }

    public void setInMemoryHandler(IFeedFrameHandler inMemoryHandler) {
        this.inMemoryHandler = inMemoryHandler;
    }

    public IFeedFrameHandler getDiskSpillHandler() {
        return diskSpillHandler;
    }

    public IFeedMemoryManager getMemoryManager() {
        return memoryManager;
    }

}