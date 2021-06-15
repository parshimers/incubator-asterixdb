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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.counters.ICounter;

public class TimedFrameWriter extends AbstractTimedTask implements IFrameWriter, IPassableTimer {

    // The downstream data consumer of this writer.
    private final IFrameWriter writer;
    final String name;

    public TimedFrameWriter(IFrameWriter writer, IStatsCollector collector, String name, ICounter counter,
            ActivityId root) {
        super(collector,counter,root);
        this.writer = writer;
        this.name = name;
    }

    @Override
    public final void open() throws HyracksDataException {
        try {
            startClock();
            writer.open();
        } finally {
            stopClock();
        }
    }

    @Override
    public final void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            startClock();
            writer.nextFrame(buffer);
        } finally {
            stopClock();
        }
    }

    @Override
    public final void flush() throws HyracksDataException {
        try {
            startClock();
            writer.flush();
        } finally {
            stopClock();
        }
    }

    @Override
    public final void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            startClock();
            writer.close();
        } finally {
            stopClock();
        }
    }

}
