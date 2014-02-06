package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.logging.Level;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class FeedFrameCollector extends MessageReceiver<DataBucket> implements IMessageReceiver<DataBucket> {

    private FeedPolicyAccessor fpa;
    private IFeedFrameWriter frameWriter;
    private State state;

    public enum State {
        ACTIVE,
        FINISHED,
        TRANSITION
    }

    public FeedFrameCollector(FeedPolicyAccessor feedPolicyAccessor, IFeedFrameWriter frameWriter) {
        super();
        this.fpa = feedPolicyAccessor;
        this.frameWriter = frameWriter;
        this.state = State.ACTIVE;
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
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Disconnected feed frame collector for " + frameWriter.getFeedId());
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

    public IFeedFrameWriter getFrameWriter() {
        return frameWriter;
    }

    public void setFrameWriter(IFeedFrameWriter frameWriter) {
        this.frameWriter = frameWriter;
    }

    @Override
    public String toString() {
        return "FrameCollector [" + frameWriter.getFeedId() + "," + state + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof FeedFrameCollector) {
            return ((FeedFrameCollector) o).getFrameWriter().getFeedId().equals(this.getFrameWriter().getFeedId());
        }
        return false;
    }

}