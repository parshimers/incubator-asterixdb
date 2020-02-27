package org.apache.hyracks.algebricks.runtime.evaluators;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;

import java.nio.ByteBuffer;

public interface IGarbage {

    void nextFrame(ByteBuffer buffer, FrameTupleReference tupleRef, IFrameTupleAccessor accessor);

}
