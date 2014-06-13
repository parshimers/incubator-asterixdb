package edu.uci.ics.asterix.common.feeds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FeedFrameDiscarder {

    private static final Logger LOGGER = Logger.getLogger(FeedFrameSpiller.class.getName());

    private final FeedRuntimeInputHandler inputHandler;
    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final FeedPolicyAccessor policyAccessor;
    private final float maxFractionDiscard;
    private int nDiscarded;

    public FeedFrameDiscarder(FeedConnectionId connectionId, FeedRuntimeId runtimeId, int frameSize,
            FeedPolicyAccessor policyAccessor, FeedRuntimeInputHandler inputHandler) throws IOException {
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.policyAccessor = policyAccessor;
        this.inputHandler = inputHandler;
        this.maxFractionDiscard = policyAccessor.getMaxFractionDiscard();
    }

    public boolean processMessage(ByteBuffer message) {
        if (policyAccessor.getMaxFractionDiscard() != 0) {
            long nProcessed = inputHandler.getProcessed();
            long discardLimit = (long) (nProcessed * maxFractionDiscard);
            if (nDiscarded >= discardLimit) {
                return false;
            }
            nDiscarded++;
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Discarded frame by " + connectionId + " (" + runtimeId + ")" + " count so far  ("
                        + nDiscarded + ") Limit [" + discardLimit + "]");
            }
            return true;
        }
        return false;
    }

}
