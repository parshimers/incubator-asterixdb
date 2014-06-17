package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

public interface IFramePostProcessor {

    public void postProcessFrame(ByteBuffer frame);
}
