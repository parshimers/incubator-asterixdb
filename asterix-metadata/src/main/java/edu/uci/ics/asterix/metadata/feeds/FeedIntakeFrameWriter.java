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
package edu.uci.ics.asterix.metadata.feeds;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.IFeedSubscriptionManager;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * A wrapper around the standard frame writer provided to an operator node pushable.
 * The wrapper monitors the flow of data from this operator to a downstream operator
 * over a connector. It collects statistics if required by the feed ingestion policy
 * and reports them to the Super Feed Manager chosen for the feed. In addition any
 * congestion experienced by the operator is also reported.
 */
public class FeedIntakeFrameWriter implements IFrameWriter {

    private static final Logger LOGGER = Logger.getLogger(FeedIntakeFrameWriter.class.getName());

    /** Actual frame writer provided to an operator. **/
    private IFrameWriter writer;

    /** The node pushable associated with the operator **/
    private IOperatorNodePushable nodePushable;

    /** A buffer for keeping frames that are waiting to be processed **/
    private List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

    /**
     * Mode associated with the frame writer
     * Possible values: FORWARD, STORE
     * 
     * @see Mode
     */
    private Mode mode;

    public enum Mode {
        /**
         * **
         * Normal mode of operation for an operator when
         * frames are pushed to the downstream operator.
         */
        FORWARD,

        /**
         * Failure mode of operation for an operator when
         * input frames are not pushed to the downstream operator but
         * are buffered for future retrieval. This mode is adopted
         * during failure recovery.
         */
        STORE
    }

    private final IFeedSubscriptionManager feedIngestionManager;

    public FeedIntakeFrameWriter(IFrameWriter writer, IOperatorNodePushable nodePushable, FeedId feedId, String nodeId,
            int partition, IFeedSubscriptionManager feedIngestionManager) {
        this.writer = writer;
        this.mode = Mode.FORWARD;
        this.nodePushable = nodePushable;
        this.feedIngestionManager = feedIngestionManager;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode newMode) throws HyracksDataException {
        if (this.mode.equals(newMode)) {
            return;
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Switching to :" + newMode + " from " + this.mode);
        }
        this.mode = newMode;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        switch (mode) {
            case FORWARD:
                writer.nextFrame(buffer);
                if (frames.size() > 0) {
                    for (ByteBuffer buf : frames) {
                        writer.nextFrame(buf);
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Flushed old frame (from previous failed execution) : " + buf
                                    + " on behalf of " + nodePushable.getDisplayName());
                        }
                    }
                    frames.clear();
                }
                break;
            case STORE:

                /* TODO:
                 * Limit the in-memory space utilized during the STORE mode. The limit (expressed in bytes) 
                 * is a parameter specified as part of the feed ingestion policy. Below is a basic implemenation
                 * that allocates a buffer on demand.   
                 * */

                ByteBuffer storageBuffer = ByteBuffer.allocate(buffer.capacity());
                storageBuffer.put(buffer);
                frames.add(storageBuffer);
                storageBuffer.flip();
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Stored frame for " + nodePushable.getDisplayName());
                }
                break;
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }

    public IFrameWriter getWriter() {
        return writer;
    }

    public void setWriter(IFrameWriter writer) {
        this.writer = writer;
    }

    @Override
    public String toString() {
        return "MaterializingFrameWriter using " + writer;
    }

    public List<ByteBuffer> getStoredFrames() {
        return frames;
    }

    public void clear() {
        frames.clear();
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

}
