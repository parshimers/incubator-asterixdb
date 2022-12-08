/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.com.job.profiling.counters.MultiResolutionEventProfiler;
import org.apache.hyracks.api.comm.FrameConstants;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.util.IntSerDeUtils;

public class ProfiledFrameWriter implements IFrameWriter, IPassableTimer {

    // The downstream data consumer of this writer.
    private final IFrameWriter writer;
    protected long frameStart = -1;
    final ICounter timeCounter;
    final ICounter tupleCounter;
    final ICounter frameCounter;
    final IStatsCollector collector;
    final IOperatorStats stats;
    final IOperatorStats parentStats;
    private int minSz = Integer.MAX_VALUE;
    private int maxSz = -1;
    private long avgSz;
    final String name;

    private MultiResolutionEventProfiler profiler;

    public ProfiledFrameWriter(IFrameWriter writer, IStatsCollector collector, String name, IOperatorStats stats,
            IOperatorStats parentStats) {
        this.writer = writer;
        this.collector = collector;
        this.name = name;
        this.stats = stats;
        this.parentStats = parentStats;
        this.timeCounter = stats.getTimeCounter();
        this.tupleCounter = parentStats != null ? parentStats.getTupleCounter() : null;
        this.frameCounter = parentStats != null ? parentStats.getFrameCounter() : null;
        this.profiler = parentStats != null ? parentStats.getProfiler() : null;
    }

    @Override
    public final void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public final void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            int tupleCountOffset = FrameHelper.getTupleCountOffset(buffer.limit());
            int tupleCount = IntSerDeUtils.getInt(buffer.array(), tupleCountOffset);
            if (tupleCounter != null) {
                long prevCount = tupleCounter.get();
                for (int i = 0; i < tupleCount; i++) {
                    int tupleLen = getTupleLength(i, tupleCountOffset, buffer);
                    if (maxSz < tupleLen) {
                        maxSz = tupleLen;
                    }
                    if (minSz > tupleLen) {
                        minSz = tupleLen;
                    }
                    long prev = avgSz * prevCount;
                    avgSz = (prev + tupleLen) / (prevCount + 1);
                    prevCount++;
                }
                parentStats.getMaxTupleSz().set(maxSz);
                parentStats.getMinTupleSz().set(minSz);
                parentStats.getAverageTupleSz().set(avgSz);
                tupleCounter.update(tupleCount);
                frameCounter.update(1);
            }
//            startClock();
            writer.nextFrame(buffer);
            if(profiler != null){profiler.reportEvent();}
        } finally {
//            stopClock();
        }
    }

    @Override
    public final void flush() throws HyracksDataException {
        try {
            writer.flush();
        } finally {
        }
    }

    @Override
    public final void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writer.close();
        } finally {
        }
    }

    protected void stopClock() {
        long t = System.nanoTime();
        long delta = t - frameStart;
        timeCounter.update(delta);
        frameStart = System.nanoTime();
    }

    protected void startClock() {
        if (frameStart != -1) {
            long t = System.nanoTime();
            long delta = t - frameStart;
            timeCounter.update(delta);
        }
        frameStart = System.nanoTime();
    }

    @Override
    public void resume() {
    }

    @Override
    public void pause() {
    }

    private int getTupleStartOffset(int tupleIndex, int tupleCountOffset, ByteBuffer buffer) {
        return tupleIndex == 0 ? FrameConstants.TUPLE_START_OFFSET
                : IntSerDeUtils.getInt(buffer.array(), tupleCountOffset - FrameConstants.SIZE_LEN * tupleIndex);
    }

    private int getTupleEndOffset(int tupleIndex, int tupleCountOffset, ByteBuffer buffer) {
        return IntSerDeUtils.getInt(buffer.array(), tupleCountOffset - FrameConstants.SIZE_LEN * (tupleIndex + 1));
    }

    public int getTupleLength(int tupleIndex, int tupleCountOffset, ByteBuffer buffer) {
        return getTupleEndOffset(tupleIndex, tupleCountOffset, buffer)
                - getTupleStartOffset(tupleIndex, tupleCountOffset, buffer);
    }

    public static IFrameWriter time(IFrameWriter writer, IHyracksTaskContext ctx, String name)
            throws HyracksDataException {
        if (!(writer instanceof ProfiledFrameWriter)) {
            IStatsCollector statsCollector = ctx.getStatsCollector();
            IOperatorStats stats = new OperatorStats(name);
            statsCollector.add(stats);
            return new ProfiledFrameWriter(writer, ctx.getStatsCollector(), name, stats, null);

        } else
            return writer;
    }
}
