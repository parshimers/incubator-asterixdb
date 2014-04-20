package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;

public class FeedFrameDiscarder {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameSpiller.class.getName());

    private final FeedOperatorInputSideHandler inputHandler;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor policyAccessor;
    private final float maxFractionDiscard;
    private int nDiscarded;

    public FeedFrameDiscarder(FeedRuntimeId runtimeId, int frameSize, FeedPolicyAccessor policyAccessor,
            FeedOperatorInputSideHandler inputHandler) throws IOException {
        this.runtimeId = runtimeId;
        this.policyAccessor = policyAccessor;
        this.inputHandler = inputHandler;
        this.maxFractionDiscard = policyAccessor.getMaxFractionDiscard();
    }

    public boolean processMessage(ByteBuffer message) {
        long nProcessed = inputHandler.getProcessed();
        long discardLimit = (long) (nProcessed * maxFractionDiscard);
        if (nDiscarded >= discardLimit) {
            return false;
        }
        nDiscarded++;
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning("Discarded frame by " + runtimeId + " count so far  (" + nDiscarded + ") Limit ["
                    + discardLimit + "]");
        }
        return true;
    }

}
