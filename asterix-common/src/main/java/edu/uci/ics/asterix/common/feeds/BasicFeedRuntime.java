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

    public BasicFeedRuntime(FeedConnectionId feedConnectionId, int partition, IFeedFrameWriter frameWriter,
            FeedRuntimeType feedRuntimeType) {
        this.feedRuntimeId = new FeedRuntimeId(feedConnectionId, feedRuntimeType, partition);
        this.frameWriter = frameWriter;
    }

    public void setFrameWriter(IFeedFrameWriter frameWriter) {
        this.frameWriter = frameWriter;
    }

    @Override
    public String toString() {
        return feedRuntimeId + " " + "runtime state present ? " + (runtimeState != null);
    }

    public static class FeedRuntimeState {

        private ByteBuffer frame;
        private IFrameWriter frameWriter;
        private Exception exception;

        public FeedRuntimeState(ByteBuffer frame, IFrameWriter frameWriter, Exception exception) {
            this.frame = frame;
            this.frameWriter = frameWriter;
            this.exception = exception;
        }

        public ByteBuffer getFrame() {
            return frame;
        }

        public void setFrame(ByteBuffer frame) {
            this.frame = frame;
        }

        public IFrameWriter getFrameWriter() {
            return frameWriter;
        }

        public void setFrameWriter(IFrameWriter frameWriter) {
            this.frameWriter = frameWriter;
        }

        public Exception getException() {
            return exception;
        }

        public void setException(Exception exception) {
            this.exception = exception;
        }

    }

    public static class FeedRuntimeId {

        public static final String DEFAULT_OPERATION_ID = "N/A";

        private final FeedRuntimeType feedRuntimeType;
        private final FeedConnectionId feedConnectionId;
        private final String operandId;
        private final int partition;
        private final int hashCode;

        public FeedRuntimeId(FeedConnectionId connectionId, FeedRuntimeType runtimeType, String operandId, int partition) {
            this.feedRuntimeType = runtimeType;
            this.operandId = operandId;
            this.feedConnectionId = connectionId;
            this.partition = partition;
            this.hashCode = (feedConnectionId + "[" + partition + "]" + feedRuntimeType).hashCode();
        }

        public FeedRuntimeId(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition) {
            this.feedRuntimeType = runtimeType;
            this.feedConnectionId = connectionId;
            this.operandId = DEFAULT_OPERATION_ID;
            this.partition = partition;
            this.hashCode = (connectionId + "[" + partition + "]" + feedRuntimeType).hashCode();
        }

        @Override
        public String toString() {
            return feedConnectionId + "[" + partition + "]" + " " + feedRuntimeType + "(" + operandId + ")";
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof FeedRuntimeId) {
                FeedRuntimeId oid = ((FeedRuntimeId) o);
                return oid.getFeedConnectionId().equals(feedConnectionId) && oid.getFeedRuntimeType().equals(feedRuntimeType)
                        && oid.getOperandId().equals(operandId) && oid.getPartition() == partition;
            }
            return false;
        }

        public FeedRuntimeType getFeedRuntimeType() {
            return feedRuntimeType;
        }

        public FeedConnectionId getFeedConnectionId() {
            return feedConnectionId;
        }

        public String getOperandId() {
            return operandId;
        }

        public int getPartition() {
            return partition;
        }

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
        return this.feedRuntimeId.getFeedConnectionId().getFeedId();
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
}
