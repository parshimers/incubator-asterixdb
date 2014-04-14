package edu.uci.ics.asterix.common.feeds;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.FrameDataException;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class FeedExceptionHandler implements IExceptionHandler {

    private static final Logger LOGGER = Logger.getLogger(FeedExceptionHandler.class.getName());

    private final int frameSize;
    private final FrameTupleAccessor fta;
    private final RecordDescriptor recordDesc;
    private final IFeedManager feedManager;
    private final FeedConnectionId connectionId;

    public FeedExceptionHandler(int frameSize, FrameTupleAccessor fta, RecordDescriptor recordDesc,
            IFeedManager feedManager, FeedConnectionId connectionId) {
        this.frameSize = frameSize;
        this.fta = fta;
        this.recordDesc = recordDesc;
        this.feedManager = feedManager;
        this.connectionId = connectionId;
    }

    public ByteBuffer handleException(Exception e, ByteBuffer frame) throws HyracksDataException {
        try {
            if (e instanceof FrameDataException) {
                fta.reset(frame);
                FrameDataException fde = (FrameDataException) e;
                int tupleIndex = fde.getTupleIndex();

                // logging 
                ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream di = new DataInputStream(bbis);

                int start = fta.getTupleStartOffset(tupleIndex) + fta.getFieldSlotsLength();
                bbis.setByteBuffer(fta.getBuffer(), start);

                Object[] record = new Object[recordDesc.getFieldCount()];

                for (int i = 0; i < record.length; ++i) {
                    Object instance = recordDesc.getFields()[i].deserialize(di);
                    if (i == 0) {
                        String tuple = String.valueOf(instance);
                        feedManager.getFeedMetadataManager().logTuple(connectionId, tuple, e.getMessage(), feedManager);
                    } else {
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning(", " + String.valueOf(instance));
                        }
                    }
                }

                // slicing
                FrameTupleAppender appender = new FrameTupleAppender(frameSize);
                ByteBuffer slicedFrame = ByteBuffer.allocate(frameSize);
                appender.reset(slicedFrame, true);
                int startTupleIndex = tupleIndex + 1;
                int totalTuples = fta.getTupleCount();
                for (int ti = startTupleIndex; ti < totalTuples; ti++) {
                    appender.append(fta, ti);
                }
                return slicedFrame;
            } else {

                return null;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to handle (log + continue) post exception " + exception.getMessage());
            }
            exception.addSuppressed(e);
            throw new HyracksDataException(exception);
        }
    }
}
