/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.exceptions.FrameDataException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class FeedFrameProcessor {

    private static Logger LOGGER = Logger.getLogger(FeedFrameProcessor.class.getName());

    public static void nextFrame(FeedConnectionId feedConnectionId, IFrameWriter writer, ByteBuffer frame,
            boolean recoverSoftFailure, int frameSize, FrameTupleAccessor fta, RecordDescriptor recordDescriptor,
            IFeedManager feedManager) throws HyracksDataException {
        boolean finishedProcessing = false;
        while (!finishedProcessing) {
            try {
                writer.nextFrame(frame);
                finishedProcessing = true;
            } catch (Exception e) {
                if (recoverSoftFailure) {
                    fta.reset(frame);
                    frame = handleException(e, frameSize, fta, recordDescriptor, feedManager, feedConnectionId);
                    if (frame == null) {
                        finishedProcessing = true;
                        if (LOGGER.isLoggable(Level.WARNING)) {
                            LOGGER.warning("Encountered exception! " + e.getMessage()
                                    + "Insufficient information, Cannot extract failing tuple");
                        }
                    }
                } else {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Ingestion policy does not require recovering from tuple. Feed would terminate");
                    }
                    throw e;
                }
            }
        }
    }

    private static ByteBuffer handleException(Exception e, int frameSize, FrameTupleAccessor fta,
            RecordDescriptor recordDesc, IFeedManager feedManager, FeedConnectionId feedConnectionId)
            throws HyracksDataException {
        try {
            if (e instanceof FrameDataException) {
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
                        feedManager.getFeedMetadataManager().logTuple(feedConnectionId, tuple, e.getMessage(),
                                feedManager);
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
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to handle (log + continue) post exception" + exception.getMessage());
            }
            exception.addSuppressed(e);
            throw new HyracksDataException(exception);
        }
    }
}
