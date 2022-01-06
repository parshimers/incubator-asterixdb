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

import java.util.HashMap;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.rewriter.runtime.SuperActivityOperatorNodePushable;

public class TimedOperatorNodePushable extends TimedFrameWriter implements IOperatorNodePushable, IPassableTimer {

    IOperatorNodePushable op;
    ActivityId acId;
    HashMap<Integer, IFrameWriter> inputs;
    long frameStart;

    TimedOperatorNodePushable(IOperatorNodePushable op, ActivityId acId, IStatsCollector collector, ICounter counter,
            ActivityId parent) throws HyracksDataException {
        super(null, collector, acId.toString() + " - " + op.getDisplayName(), counter, parent);
        this.op = op;
        this.acId = acId;
        inputs = new HashMap<>();
    }

    @Override
    public void initialize() throws HyracksDataException {
        startClock();
        op.initialize();
        stopClock();
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        startClock();
        op.deinitialize();
        stopClock();
    }

    @Override
    public int getInputArity() {
        return op.getInputArity();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        op.setOutputFrameWriter(index, writer, recordDesc);
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        IFrameWriter ifw = op.getInputFrameWriter(index);
        if (!(op instanceof TimedFrameWriter) && ifw.equals(op)) {
            return new TimedFrameWriter(op.getInputFrameWriter(index), collector,
                    acId.toString() + "-" + op.getDisplayName(), counter, root);
        }
        return op.getInputFrameWriter(index);
    }

    @Override
    public String getDisplayName() {
        return op.getDisplayName();
    }

    private void stopClock() {
        pause();
        collector.giveClock(this, root);
    }

    private void startClock() {
        if (frameStart > 0) {
            return;
        }
        frameStart = collector.takeClock(this, root);
    }

    @Override
    public void resume() {
        if (frameStart > 0) {
            return;
        }
        long nt = System.nanoTime();
        frameStart = nt;
    }

    @Override
    public void pause() {
        if (frameStart > 0) {
            long nt = System.nanoTime();
            long delta = nt - frameStart;
            counter.update(delta);
            frameStart = -1;
        }
    }

    public static IOperatorNodePushable time(IOperatorNodePushable op, IHyracksTaskContext ctx, ActivityId acId)
            throws HyracksDataException {
        if (!(op instanceof TimedOperatorNodePushable) && !(op instanceof SuperActivityOperatorNodePushable)) {
            String name = acId.toString() + " - " + op.getDisplayName();
            IStatsCollector statsCollector = ctx.getStatsCollector();
            IOperatorStats stats = new OperatorStats(name);
            statsCollector.add(stats);
            return new TimedOperatorNodePushable(op, acId, ctx.getStatsCollector(), stats.getTimeCounter(), acId);
        }
        return op;
    }
}
