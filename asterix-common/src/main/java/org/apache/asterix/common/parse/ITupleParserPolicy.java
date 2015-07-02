package org.apache.asterix.common.parse;

import java.util.Map;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface ITupleParserPolicy {

    public enum TupleParserPolicy {
        FRAME_FULL,
        TIME_COUNT_ELAPSED,
        RATE_CONTROLLED
    }

    public TupleParserPolicy getType();

    public void configure(Map<String, String> configuration) throws HyracksDataException;

    public void initialize(IHyracksTaskContext ctx, IFrameWriter writer) throws HyracksDataException;

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    public void close() throws HyracksDataException;
}
