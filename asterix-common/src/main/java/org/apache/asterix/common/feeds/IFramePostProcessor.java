package org.apache.asterix.common.feeds;

import java.nio.ByteBuffer;

import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public interface IFramePostProcessor {

    public void postProcessFrame(ByteBuffer frame, FrameTupleAccessor frameAccessor);
}
