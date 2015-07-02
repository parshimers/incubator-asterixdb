package org.apache.asterix.common.parse;

import java.util.Map;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public interface ITupleForwardPolicy {

    public static final String PARSER_POLICY = "parser-policy";
    
    public enum TupleForwardPolicyType {
        FRAME_FULL,
        COUNTER_TIMER_EXPIRED,
        RATE_CONTROLLED
    }

    public void configure(Map<String, String> configuration);

    public void initialize(IHyracksTaskContext ctx, IFrameWriter frameWriter) throws HyracksDataException;

    public TupleForwardPolicyType getType();

    public void addTuple(ArrayTupleBuilder tb) throws HyracksDataException;

    public void close() throws HyracksDataException;

}
