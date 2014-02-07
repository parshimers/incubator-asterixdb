package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector extends MessageReceiver<DataBucket> implements IMessageReceiver<DataBucket> {

    private FeedPolicyAccessor fpa;
    private final FeedId feedId;
    private IFrameWriter frameWriter;
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

    public FeedFrameCollector(FeedPolicyAccessor feedPolicyAccessor, IFeedFrameWriter frameWriter, FeedId feedId) {
        super();
        this.fpa = feedPolicyAccessor;
        this.feedId = feedId;
        this.frameWriter = frameWriter;
        this.state = State.ACTIVE;
        this.mode = Mode.FORWARD_TO_WRITER;
    }

    public FeedFrameCollector(FeedPolicyAccessor feedPolicyAccessor, IFrameWriter frameWriter, FeedId feedId) {
        super();
        this.fpa = feedPolicyAccessor;
        this.feedId = feedId;
        this.frameWriter = frameWriter;
        this.state = State.ACTIVE;
        this.mode = Mode.FORWARD_TO_OPERATOR;
    }

    @Override
    public void processMessage(DataBucket bucket) throws Exception {
        try {
            switch (bucket.getContentType()) {
                case DATA:
                    frameWriter.nextFrame(bucket.getBuffer());
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
            super.close(false);
            disconnect();
        }
    }

    public synchronized void disconnect() {
        setState(State.FINISHED);
        notifyAll();
        if (mode.equals(Mode.FORWARD_TO_WRITER)) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Disconnected feed frame collector for " + ((IFeedFrameWriter) frameWriter).getFeedId());
            }
        }
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

    public void setFrameWriter(IFeedFrameWriter frameWriter) {
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

}