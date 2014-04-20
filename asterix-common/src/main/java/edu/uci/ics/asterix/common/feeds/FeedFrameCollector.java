/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * Copyright 2009-2013 by The Regents of the University of California
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import edu.uci.ics.asterix.common.feeds.api.IFeedOperatorOutputSideHandler;
import edu.uci.ics.asterix.common.feeds.api.IMessageReceiver;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector extends MessageReceiver<DataBucket> implements IMessageReceiver<DataBucket> {

    private final FrameDistributor frameDistributor;
    private FeedPolicyAccessor fpa;
    private final FeedId feedId;
    private IFrameWriter frameWriter;
    private FeedOperatorInputSideHandler frameProcessor;
    private State state;
    private final Mode mode;

    public enum State {
        ACTIVE,
        FINISHED,
        TRANSITION
    }

    public enum Mode {
        FORWARD_TO_WRITER,
        FORWARD_TO_OPERATOR
    }

    public FeedFrameCollector(FrameDistributor frameDistributor, FeedPolicyAccessor feedPolicyAccessor,
            IFeedOperatorOutputSideHandler frameWriter, FeedId feedId) {
        super();
        this.frameDistributor = frameDistributor;
        this.fpa = feedPolicyAccessor;
        this.feedId = feedId;
        this.frameWriter = frameWriter;
        this.state = State.ACTIVE;
        this.mode = Mode.FORWARD_TO_WRITER;
    }

    public FeedFrameCollector(FrameDistributor frameDistributor, FeedPolicyAccessor feedPolicyAccessor,
            FeedOperatorInputSideHandler frameProcessor, FeedId feedId) {
        super();
        this.frameDistributor = frameDistributor;
        this.fpa = feedPolicyAccessor;
        this.feedId = feedId;
        this.frameProcessor = frameProcessor;
        this.state = State.ACTIVE;
        this.mode = Mode.FORWARD_TO_OPERATOR;
    }

    @Override
    public void processMessage(DataBucket bucket) throws Exception {
        try {
            ByteBuffer frame = bucket.getBuffer();
            switch (bucket.getContentType()) {
                case DATA:
                    switch (mode) {
                        case FORWARD_TO_OPERATOR:
                            frameProcessor.nextFrame(frame);
                            break;
                        case FORWARD_TO_WRITER:
                            frameWriter.nextFrame(frame);
                            break;
                    }
                    break;
                case EOD:
                    closeCollector();
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to process data bucket " + bucket + ", encountered exception " + e.getMessage());
            }
        } finally {
            bucket.doneReading();
        }
    }

    public void closeCollector() {
        if (state.equals(State.TRANSITION)) {
            super.close(true);
            setState(State.ACTIVE);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(this + " is now " + State.ACTIVE + " mode, processing frames synchronously");
            }
        } else {
            flushPendingMessages();
            setState(State.FINISHED);
            synchronized (frameDistributor.getRegisteredCollectors()) {
                frameDistributor.getRegisteredCollectors().notifyAll();
            }
            disconnect();
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Closed collector " + this);
        }
    }

    public synchronized void disconnect() {
        setState(State.FINISHED);
        notifyAll();
    }

    public synchronized void nextFrame(ByteBuffer frame) throws HyracksDataException {
        frameWriter.nextFrame(frame);
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return fpa;
    }

    public synchronized State getState() {
        return state;
    }

    public synchronized void setState(State state) {
        this.state = state;
    }

    public IFrameWriter getFrameWriter() {
        return frameWriter;
    }

    public void setFrameWriter(IFeedOperatorOutputSideHandler frameWriter) {
        this.frameWriter = frameWriter;
    }

    public Mode getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "FrameCollector (" + mode + ")" + feedId + "," + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FeedFrameCollector) {
            return feedId.equals(((FeedFrameCollector) o).feedId);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return feedId.toString().hashCode();
    }

    public FeedOperatorInputSideHandler getFrameProcessor() {
        return frameProcessor;
    }

}