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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hyracks.api.HyracksRunnable;
import org.apache.hyracks.api.com.job.profiling.counters.Counter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.job.profiling.IStatsCollector;
import org.apache.hyracks.api.job.profiling.OperatorStats;
import org.apache.hyracks.api.job.profiling.counters.ICounter;
import org.apache.hyracks.api.rewriter.runtime.SuperActivityOperatorNodePushable;

public class ProfiledOperatorNodePushable implements IOperatorNodePushable {

    IOperatorNodePushable op;
    ActivityId acId;
    HashMap<Integer, ProfiledFrameWriter> inputs;
    HashMap<Integer, ProfiledOperatorNodePushable> parents;

    List<ProfiledFrameWriter> outputs;
    IOperatorStats stats;
    IStatsCollector collector;

    ICounter time;

    ProfiledOperatorNodePushable(IOperatorNodePushable op, ActivityId acId, IStatsCollector collector,
            IOperatorStats stats, ProfiledOperatorNodePushable parentOp) throws HyracksDataException {
        this.stats = stats;
        this.collector = collector;
        this.parents = new HashMap<>();
        parents.put(0, parentOp);
        this.op = op;
        this.acId = acId;
        inputs = new HashMap<>();
        outputs = new ArrayList<>();
        this.time = new Counter("shadowTime");
    }

    void time(HyracksRunnable r) throws HyracksDataException {
        long nt = 0;
        try {
            nt = System.nanoTime();
            r.run();
        } finally {
            time.update(System.nanoTime() - nt);
        }
    }

    @Override
    public void initialize() throws HyracksDataException {
       time(op::initialize);
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        long totalTime = time.get();
        for(ProfiledFrameWriter i: inputs.values()){
           totalTime += i.time.get();
        }
        for(ProfiledFrameWriter w: outputs){
            totalTime -= w.time.get();
        }
        op.deinitialize();
        stats.getTimeCounter().set(totalTime);
    }

    @Override
    public int getInputArity() {
        return op.getInputArity();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
            throws HyracksDataException {
        //If we have no parent, that means we are a source, and so we need to ask our child to
        //track the time between our invocations of their nextFrame() to infer our time spent
        if (writer instanceof ProfiledFrameWriter) {
            ProfiledFrameWriter wrapper = (ProfiledFrameWriter) writer;
            outputs.add(wrapper);
        }
        op.setOutputFrameWriter(index, writer, recordDesc);
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        if (inputs.get(index) == null){
            ProfiledFrameWriter pfw = new ProfiledFrameWriter(op.getInputFrameWriter(index), collector,
                    acId.toString() + "-" + op.getDisplayName(), stats, null);
            inputs.put(index, pfw);
            return pfw;
        }
        else {
            return inputs.get(index);
        }
    }

    @Override
    public String getDisplayName() {
        return op.getDisplayName();
    }

    public void addParent(int index, ProfiledOperatorNodePushable parent) {
        parents.put(index, parent);
    }

    public IOperatorStats getStats() {
        return stats;
    }

    public static IOperatorNodePushable time(IOperatorNodePushable op, IHyracksTaskContext ctx, ActivityId acId,
            ProfiledOperatorNodePushable source) throws HyracksDataException {
        String name = acId.toString() + " - " + op.getDisplayName();
        IStatsCollector statsCollector = ctx.getStatsCollector();
        IOperatorStats stats = new OperatorStats(name, acId.getOperatorDescriptorId());
        statsCollector.add(stats);
        if (op instanceof IIntrospectingOperator) {
            ((IIntrospectingOperator) op).setOperatorStats(stats);
        }
        if (!(op instanceof ProfiledOperatorNodePushable) && !(op instanceof SuperActivityOperatorNodePushable)) {
            return new ProfiledOperatorNodePushable(op, acId, ctx.getStatsCollector(), stats, source);
        }
        return op;
    }

    public static void onlyAddStats(IOperatorNodePushable op, IHyracksTaskContext ctx, ActivityId acId)
            throws HyracksDataException {
        String name = acId.toString() + " - " + op.getDisplayName();
        IStatsCollector statsCollector = ctx.getStatsCollector();
        IOperatorStats stats = new OperatorStats(name, acId.getOperatorDescriptorId());
        if (op instanceof IIntrospectingOperator) {
            ((IIntrospectingOperator) op).setOperatorStats(stats);
            statsCollector.add(stats);
        }
    }
}
