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

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.DataBucket.ContentType;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

// Handles Exception + inflow rate measurement
public class FeedOperatorInputSideHandler {

    private static Logger LOGGER = Logger.getLogger(FeedOperatorInputSideHandler.class.getName());

    private final FeedConnectionId connectionId;
    private final int frameSize;
    private final FrameTupleAccessor fta;
    private final IFrameWriter coreOperator;
    private final RecordDescriptor recordDesc;
    private final IFeedManager feedManager;
    private final FeedPolicyAccessor feedPolicyAccessor;
    private final boolean bufferingEnabled;
    private MonitoredBuffer mBuffer;
    private final FeedRuntimeType runtimeType;
    private final int partition;
    private DataBucketPool pool;
    private final IExceptionHandler exceptionHandler;
    private boolean finished;

    public FeedOperatorInputSideHandler(FeedConnectionId connectionId, final IFrameWriter coreOperator,
            FeedRuntimeType runtimeType, int partition, FeedPolicyAccessor fpa, boolean bufferingEnabled,
            int frameSize, FrameTupleAccessor fta, RecordDescriptor recordDesc, IFeedManager feedManager) {
        this.connectionId = connectionId;
        this.coreOperator = coreOperator;
        this.bufferingEnabled = bufferingEnabled;
        this.feedPolicyAccessor = fpa;
        this.frameSize = frameSize;
        this.fta = fta;
        this.recordDesc = recordDesc;
        this.runtimeType = runtimeType;
        this.partition = partition;
        this.feedManager = feedManager;
        this.exceptionHandler = new FeedExceptionHandler(frameSize, fta, recordDesc, feedManager, connectionId);
        if (bufferingEnabled) {
            mBuffer = new MonitoredBuffer(coreOperator, fta, feedManager.getFeedMetricCollector(), connectionId,
                    runtimeType, partition, exceptionHandler, fpa, new IFrameEventCallback() {

                        @Override
                        public void frameEvent(FrameEvent frameEvent) throws HyracksDataException {
                            finished = true;
                            synchronized (coreOperator) {
                                coreOperator.notifyAll();
                            }
                        }

                    });
            pool = (DataBucketPool) feedManager.getFeedMemoryManager().getMemoryComponent(
                    IFeedMemoryComponent.Type.POOL);
            mBuffer.start();
        }
        this.finished = false;

    }

    public void nextFrame(ByteBuffer frame) throws HyracksDataException {
        boolean finishedProcessing = false;
        while (!finishedProcessing) {
            try {
                if (bufferingEnabled) {
                    DataBucket bucket = pool.getDataBucket();
                    if (frame != null) {
                        bucket.reset(frame);
                        bucket.setContentType(ContentType.DATA);
                    } else {
                        bucket.setContentType(ContentType.EOD);
                    }
                    bucket.setDesiredReadCount(1);
                    mBuffer.sendMessage(bucket);
                } else {
                    coreOperator.nextFrame(frame);
                }
                finishedProcessing = true;
            } catch (Exception e) {
                if (feedPolicyAccessor.continueOnSoftFailure()) {
                    frame = exceptionHandler.handleException(e, frame);
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
                    mBuffer.close(false);
                    throw e;
                }
            }
        }
    }

    public void close() {
        if (mBuffer != null) {
            mBuffer.close(false);
        }
    }

}
