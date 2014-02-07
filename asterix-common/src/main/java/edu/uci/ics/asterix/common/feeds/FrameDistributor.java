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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedFrameHandlers.FeedMemoryEventListener;
import edu.uci.ics.asterix.common.feeds.IFeedMemoryComponent.Type;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FrameDistributor {

    private static final Logger LOGGER = Logger.getLogger(FrameDistributor.class.getName());

    private final FeedId feedId;
    private final FeedRuntimeType feedRuntimeType;
    private final int partition;

    /** A map storing the registered frame readers ({@code FeedFrameCollector}. **/
    private final Map<IFrameWriter, FeedFrameCollector> registeredCollectors;

    private DataBucketPool pool;
    private DistributionMode distributionMode;
    private final IFeedMemoryManager memoryManager;
    private IFeedFrameHandler inMemoryHandler;
    private IFeedFrameHandler diskSpillHandler;
    private boolean enableShortCircuiting;
    private RoutingMode routingMode;
    private IMemoryEventListener mListener;
    private boolean spillToDiskRequired = false;

    public static enum RoutingMode {
        IN_MEMORY_ROUTE,
        SPILL_TO_DISK,
        DISCARD
    }

    public enum DistributionMode {
        /**
         * A single feed frame collector is registered for receiving tuples.
         * Tuple is sent via synchronous call, that is no buffering is involved
         **/
        SINGLE,

        /** Multiple feed frame collectors are concurrently registered for receiving tuples. **/
        SHARED,

        /** Feed tuples are not being processed, irrespective of # of registered feed frame collectors. **/
        INACTIVE
    }

    public FrameDistributor(FeedId feedId, FeedRuntimeType feedRuntimeType, int partition,
            boolean enableShortCircuiting, IFeedMemoryManager memoryManager) throws HyracksDataException {
        this.feedId = feedId;
        this.feedRuntimeType = feedRuntimeType;
        this.partition = partition;
        this.memoryManager = memoryManager;
        this.enableShortCircuiting = enableShortCircuiting;
        this.registeredCollectors = new HashMap<IFrameWriter, FeedFrameCollector>();
        distributionMode = DistributionMode.INACTIVE;
        routingMode = RoutingMode.IN_MEMORY_ROUTE;
        try {
            inMemoryHandler = FeedFrameHandlers.getFeedFrameHandler(this, feedId, RoutingMode.IN_MEMORY_ROUTE);
            diskSpillHandler = FeedFrameHandlers.getFeedFrameHandler(this, feedId, RoutingMode.SPILL_TO_DISK);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
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
        DistributionMode currentMode = distributionMode;
        switch (distributionMode) {
            case INACTIVE:
                if (!enableShortCircuiting) {
                    pool = (DataBucketPool) memoryManager.getMemoryComponent(Type.POOL);
                    frameCollector.start();
                }
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                setMode(DistributionMode.SINGLE);
                break;
            case SINGLE:
                pool = (DataBucketPool) memoryManager.getMemoryComponent(Type.POOL);
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                for (FeedFrameCollector reader : registeredCollectors.values()) {
                    reader.start();
                }
                setMode(DistributionMode.SHARED);
                break;
            case SHARED:
                frameCollector.start();
                registeredCollectors.put(frameCollector.getFrameWriter(), frameCollector);
                break;
        }
        if (!spillToDiskRequired) {
            spillToDiskRequired = frameCollector.getFeedPolicyAccessor().spillToDiskOnCongestion();
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to " + distributionMode + " mode from " + currentMode + " mode " + " Feed id "
                    + feedId);
        }
    }

    public synchronized void deregisterFrameCollector(FeedFrameCollector frameCollector) {
        switch (distributionMode) {
            case INACTIVE:
                throw new IllegalStateException("Invalid attempt to deregister frame collector in " + distributionMode
                        + " mode.");
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

    public boolean deregisterFrameCollector(IFeedFrameWriter frameWriter) {
        FeedFrameCollector collector = registeredCollectors.remove(frameWriter);
        return collector != null;
    }

    public synchronized void setMode(DistributionMode mode) {
        this.distributionMode = mode;
    }

    public boolean isRegistered(IFeedFrameWriter writer) {
        return registeredCollectors.get(writer) != null;
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
        switch (distributionMode) {
            case INACTIVE:
                break;
            case SINGLE:
                FeedFrameCollector collector = registeredCollectors.values().iterator().next();
                switch (collector.getState()) {
                    case ACTIVE:
                        if (enableShortCircuiting) {
                            collector.nextFrame(frame); //processing is synchronous
                        } else {
                            DataBucket bucket = getDataBucket();
                            if (bucket == null) {
                                switchRoutingMode(frame); // memory congestion
                            } else {
                                bucket.reset(frame);
                                inMemoryHandler.handleDataBucket(bucket);
                            }
                        }
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

    private void handleUnRoutableFrame(ByteBuffer frame) throws HyracksDataException {

    }

    private void switchRoutingMode(ByteBuffer frame) throws HyracksDataException {
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Unable to allocate memory, will evaluate the need to spill");
        }
        boolean spillToDisk = false;
        int maxSpillSizeMB = 0;
        for (FeedFrameCollector collector : registeredCollectors.values()) {
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
        for (FeedFrameCollector collector : registeredCollectors.values()) {
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
        return distributionMode;
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

    public void close() {
        switch (distributionMode) {
            case INACTIVE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("FrameDistributor is " + distributionMode);
                }
                break;
            case SINGLE:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Disconnecting single frame reader in " + distributionMode + " mode " + " for  feedId "
                            + feedId);
                }
                setMode(DistributionMode.INACTIVE);
                registeredCollectors.values().iterator().next().disconnect();
                break;
            case SHARED:
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Signalling End Of Feed; currently operating in " + distributionMode + " mode");
                }
                notifyEndOfFeed();
                break;
        }
    }

    public Collection<FeedFrameCollector> getRegisteredCollectors() {
        return registeredCollectors.values();
    }

    public Map<IFrameWriter, FeedFrameCollector> getRegisteredReaders() {
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

    public DistributionMode getDistributionMode() {
        return distributionMode;
    }

    public FeedRuntimeType getFeedRuntimeType() {
        return feedRuntimeType;
    }

    public int getPartition() {
        return partition;
    }

}