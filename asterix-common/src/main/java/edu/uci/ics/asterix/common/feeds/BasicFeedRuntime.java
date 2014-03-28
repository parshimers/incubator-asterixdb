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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;

public class BasicFeedRuntime implements IFeedRuntime {

    /** A unique identifier for the runtime **/
    protected final FeedRuntimeId feedRuntimeId;

    /** The runtime state: @see FeedRuntimeState **/
    protected FeedRuntimeState runtimeState;

    /** The frame writer associated with the runtime **/
    protected IFeedFrameWriter frameWriter;

    public BasicFeedRuntime(FeedConnectionId connectionId, int partition, IFeedFrameWriter frameWriter,
            FeedRuntimeType runtimeType) {
        this.feedRuntimeId = new FeedRuntimeId(connectionId, runtimeType, partition);
        this.frameWriter = frameWriter;
    }

    public void setFrameWriter(IFeedFrameWriter frameWriter) {
        this.frameWriter = frameWriter;
    }

    @Override
    public String toString() {
        return feedRuntimeId + " " + "runtime state present ? " + (runtimeState != null);
    }

    public FeedRuntimeState getRuntimeState() {
        return runtimeState;
    }

    public void setRuntimeState(FeedRuntimeState runtimeState) {
        this.runtimeState = runtimeState;
    }

    public FeedRuntimeId getFeedRuntimeId() {
        return feedRuntimeId;
    }

    @Override
    public FeedId getFeedId() {
        return this.feedRuntimeId.getConnectionId().getFeedId();
    }

    @Override
    public FeedRuntimeType getFeedRuntimeType() {
        return feedRuntimeId.getFeedRuntimeType();
    }

    @Override
    public IFeedFrameWriter getFeedFrameWriter() {
        return frameWriter;
    }

    public void setFeedRuntimeState(FeedRuntimeState feedRuntimeState) {
        this.runtimeState = feedRuntimeState;
    }

    public static class FeedRuntimeState {

        private final ByteBuffer frame;
        private final IFrameWriter frameWriter;

        public FeedRuntimeState(ByteBuffer frame, IFrameWriter frameWriter) {
            this.frame = frame;
            this.frameWriter = frameWriter;
        }

        public ByteBuffer getFrame() {
            return frame;
        }

        public IFrameWriter getFrameWriter() {
            return frameWriter;
        }

    }

    public static class FeedRuntimeId {

        public static final String DEFAULT_OPERATION_ID = "N/A";

        private final FeedRuntimeType runtimeType;
        private final FeedConnectionId connectionId;
        private final String operandId;
        private final int partition;
        private final int hashCode;

        public FeedRuntimeId(FeedConnectionId connectionId, FeedRuntimeType runtimeType, String operandId, int partition) {
            this.runtimeType = runtimeType;
            this.operandId = operandId;
            this.connectionId = connectionId;
            this.partition = partition;
            this.hashCode = (connectionId + "[" + partition + "]" + runtimeType).hashCode();
        }

        public FeedRuntimeId(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition) {
            this(connectionId, runtimeType, DEFAULT_OPERATION_ID, partition);
        }

        @Override
        public String toString() {
            return connectionId + "[" + partition + "]" + " " + runtimeType + "(" + operandId + ")";
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof FeedRuntimeId) {
                FeedRuntimeId oid = ((FeedRuntimeId) o);
                return oid.getConnectionId().equals(connectionId) && oid.getFeedRuntimeType().equals(runtimeType)
                        && oid.getOperandId().equals(operandId) && oid.getPartition() == partition;
            }
            return false;
        }

        public FeedRuntimeType getFeedRuntimeType() {
            return runtimeType;
        }

        public FeedConnectionId getConnectionId() {
            return connectionId;
        }

        public String getOperandId() {
            return operandId;
        }

        public int getPartition() {
            return partition;
        }

    }

}
