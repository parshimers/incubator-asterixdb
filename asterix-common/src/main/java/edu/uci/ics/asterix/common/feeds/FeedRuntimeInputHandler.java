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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DataBucket.ContentType;
import edu.uci.ics.asterix.common.feeds.api.IExceptionHandler;
import edu.uci.ics.asterix.common.feeds.api.IFeedManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryComponent;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.Mode;
import edu.uci.ics.asterix.common.feeds.api.IFrameEventCallback;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

// Handles Exception + inflow rate measurement
public class FeedRuntimeInputHandler implements IFrameWriter {

    private static Logger LOGGER = Logger.getLogger(FeedRuntimeInputHandler.class.getName());

    private IFrameWriter coreOperator;
    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor feedPolicyAccessor;
    private final boolean bufferingEnabled;
    private MonitoredBuffer mBuffer;
    private DataBucketPool pool;
    private FrameCollection frameCollection;
    private final IExceptionHandler exceptionHandler;
    private Mode mode;
    private Mode lastMode;
    private final FeedFrameDiscarder discarder;
    private final FeedFrameSpiller spiller;
    private boolean finished;
    private long nProcessed;
    private final FrameTupleAccessor fta;
    private final FeedPolicyAccessor fpa;
    private final IFeedManager feedManager;

    public IFrameWriter getCoreOperator() {
        return coreOperator;
    }

    public void setCoreOperator(IFrameWriter coreOperator) {
        this.coreOperator = coreOperator;
    }

    public FeedRuntimeInputHandler(FeedConnectionId connectionId, FeedRuntimeId runtimeId, IFrameWriter coreOperator,
            FeedPolicyAccessor fpa, boolean bufferingEnabled, int frameSize, FrameTupleAccessor fta,
            RecordDescriptor recordDesc, IFeedManager feedManager) throws IOException {
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.coreOperator = coreOperator;
        this.bufferingEnabled = bufferingEnabled;
        this.feedPolicyAccessor = fpa;
        this.spiller = new FeedFrameSpiller(connectionId, runtimeId, frameSize, fpa);
        this.discarder = new FeedFrameDiscarder(connectionId, runtimeId, frameSize, fpa, this);
        this.exceptionHandler = new FeedExceptionHandler(frameSize, fta, recordDesc, feedManager, connectionId);
        this.mode = Mode.PROCESS;
        this.lastMode = Mode.PROCESS;
        this.finished = false;
        this.fpa = fpa;
        this.fta = fta;
        this.feedManager = feedManager;
        this.pool = (DataBucketPool) feedManager.getFeedMemoryManager().getMemoryComponent(
                IFeedMemoryComponent.Type.POOL);

        this.frameCollection = (FrameCollection) feedManager.getFeedMemoryManager().getMemoryComponent(
                IFeedMemoryComponent.Type.COLLECTION);

        //  if (bufferingEnabled) {
        mBuffer = new MonitoredBuffer(this, coreOperator, fta, feedManager.getFeedMetricCollector(), connectionId,
                runtimeId, exceptionHandler, new FrameEventCallback(fpa, this, coreOperator));
        mBuffer.start();
        //  }
    }

    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        try {
            switch (mode) {
                case PROCESS:
                    switch (lastMode) {
                        case SPILL:
                        case POST_SPILL_DISCARD:
                            setMode(Mode.PROCESS_SPILL);
                            processSpilledBacklog(); // non blocking call
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Done with replaying spilled data, will resume normal processing, backlog collected "
                                        + mBuffer.getWorkSize());
                            }
                            break;
                        case BUFFER_RECOVERY:
                            processBufferredBacklog(); // non-blocking call
                            break;
                        default:
                            break;
                    }
                    process(frame);
                    if (frame != null) {
                        nProcessed++;
                    }
                    break;

                case PROCESS_SPILL:
                    process(frame);
                    if (frame != null) {
                        nProcessed++;
                    }
                    break;
                case SPILL: {
                    boolean success = spill(frame);
                    if (!success) {
                        // sendMessage to CentralFeedManager
                        reportUnresolvableCongestion();
                    }
                    break;
                }
                case DISCARD:
                case POST_SPILL_DISCARD:
                    boolean success = discarder.processMessage(frame);
                    if (!success) {
                        // sendMessage to CentralFeedManager
                        reportUnresolvableCongestion();
                    }
                    break;
                case BUFFER_RECOVERY:
                    // buffer until the pipeline is restored
                    bufferDataDuringRecovery(frame);
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void bufferDataDuringRecovery(ByteBuffer frame) throws Exception {
        boolean success = frameCollection.addFrame(frame);
        if (!success) {
            if (fpa.spillToDiskOnCongestion()) {
                if (frame != null) {
                    spiller.processMessage(frame);
                    setMode(Mode.SPILL);
                } // TODO handle the else case
            } else {
                discarder.processMessage(frame);
                setMode(Mode.DISCARD);
            }
        }
    }

    private void reportUnresolvableCongestion() throws HyracksDataException {
        FeedCongestionMessage congestionReport = new FeedCongestionMessage(connectionId, runtimeId,
                mBuffer.getInflowRate(), mBuffer.getOutflowRate());
        feedManager.getFeedMessageService().sendMessage(congestionReport);
    }

    private void processBufferredBacklog() throws HyracksDataException {
        try {
            Iterator<ByteBuffer> backlog = frameCollection.getFrameCollectionIterator();
            while (backlog.hasNext()) {
                process(backlog.next());
                nProcessed++;
            }
            DataBucket bucket = pool.getDataBucket();
            bucket.setContentType(ContentType.EOSD);
            bucket.setDesiredReadCount(1);
            mBuffer.sendMessage(bucket);
            frameCollection.reset();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void processSpilledBacklog() throws HyracksDataException {
        try {
            Iterator<ByteBuffer> backlog = spiller.replayData();
            while (backlog.hasNext()) {
                process(backlog.next());
                nProcessed++;
            }
            DataBucket bucket = pool.getDataBucket();
            bucket.setContentType(ContentType.EOSD);
            bucket.setDesiredReadCount(1);
            mBuffer.sendMessage(bucket);
            spiller.reset();
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }
    }

    private void process(ByteBuffer frame) throws HyracksDataException {
        boolean finishedProcessing = false;
        while (!finishedProcessing) {
            try {
                if (!bufferingEnabled) {
                    coreOperator.nextFrame(frame);
                } else {
                    DataBucket bucket = pool.getDataBucket();
                    if (bucket == null) {
                        if (fpa.spillToDiskOnCongestion()) {
                            if (frame != null) {
                                spiller.processMessage(frame);
                                setMode(Mode.SPILL);
                            } // TODO handle the else case
                        } else {
                            discarder.processMessage(frame);
                            setMode(Mode.DISCARD);
                        }
                    } else {
                        if (frame != null) {
                            bucket.reset(frame); // created a copy here
                            bucket.setContentType(ContentType.DATA);
                        } else {
                            bucket.setContentType(ContentType.EOD);
                        }
                        bucket.setDesiredReadCount(1);
                        mBuffer.sendMessage(bucket);
                    }
                }
                finishedProcessing = true;
            } catch (Exception e) {
                e.printStackTrace();
                if (feedPolicyAccessor.continueOnSoftFailure()) {
                    frame = exceptionHandler.handleException(e, frame);
                    if (frame == null) {
                        finishedProcessing = true;
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Encountered exception! " + e.getMessage()
                                    + "Insufficient information, Cannot extract failing tuple");
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Ingestion policy does not require recovering from tuple. Feed would terminate");
                    }
                    mBuffer.close(false);
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    private boolean spill(ByteBuffer frame) throws Exception {
        boolean success = spiller.processMessage(frame);
        if (!success) {
            // limit reached
            setMode(Mode.POST_SPILL_DISCARD);
        }
        return success;
    }

    public Mode getMode() {
        return mode;
    }

    public synchronized void setMode(Mode mode) {
        if (!mode.equals(this.mode)) {
            this.lastMode = this.mode;
            this.mode = mode;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Switched from " + lastMode + " to " + mode);
            }
        }
    }

    public void close() {
        if (mBuffer != null) {
            mBuffer.close(false);
        }
    }

    private static class FrameEventCallback implements IFrameEventCallback {

        private static final Logger LOGGER = Logger.getLogger(FrameEventCallback.class.getName());

        private final FeedPolicyAccessor fpa;
        private final FeedRuntimeInputHandler inputSideHandler;
        private final IFrameWriter coreOperator;

        public FrameEventCallback(FeedPolicyAccessor fpa, FeedRuntimeInputHandler inputSideHandler,
                IFrameWriter coreOperator) {
            this.fpa = fpa;
            this.inputSideHandler = inputSideHandler;
            this.coreOperator = coreOperator;
        }

        @Override
        public void frameEvent(FrameEvent event) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Frame Event for " + inputSideHandler.getRuntimeId() + " " + event);
            }
            if (!event.equals(FrameEvent.FINISHED_PROCESSING_SPILLAGE)
                    && inputSideHandler.getMode().equals(Mode.PROCESS_SPILL)) {
                return;
            }
            switch (event) {
                case PENDING_WORK_THRESHOLD_REACHED:
                    if (fpa.spillToDiskOnCongestion()) {
                        inputSideHandler.setMode(Mode.SPILL);
                    } else {
                        inputSideHandler.setMode(Mode.DISCARD);
                    }
                    break;
                case FINISHED_PROCESSING:
                    inputSideHandler.setFinished(true);
                    synchronized (coreOperator) {
                        coreOperator.notifyAll();
                    }
                    break;
                case PENDING_WORK_DONE:
                    switch (inputSideHandler.getMode()) {
                        case SPILL:
                        case DISCARD:
                        case POST_SPILL_DISCARD:
                            inputSideHandler.setMode(Mode.PROCESS);
                            break;
                        default:
                            throw new IllegalStateException(" Received event type " + event);

                    }
                    break;
                case FINISHED_PROCESSING_SPILLAGE:
                    inputSideHandler.setMode(Mode.PROCESS);
                    break;
                default:
                    break;
            }
        }

    }

    public boolean isFinished() {
        return finished;
    }

    public void setFinished(boolean finished) {
        this.finished = finished;
    }

    public long getProcessed() {
        return nProcessed;
    }

    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    @Override
    public void open() throws HyracksDataException {
        coreOperator.open();
    }

    @Override
    public void fail() throws HyracksDataException {
        coreOperator.fail();
    }

    public void reset(IFrameWriter frameWriter) {
        this.coreOperator = frameWriter;
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

}
