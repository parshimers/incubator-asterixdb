package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IExceptionHandler {

    public ByteBuffer handleException(Exception e, ByteBuffer frame) throws HyracksDataException;
}
