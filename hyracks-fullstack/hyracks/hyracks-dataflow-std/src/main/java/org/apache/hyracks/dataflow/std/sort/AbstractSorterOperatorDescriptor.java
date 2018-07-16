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

package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.io.GeneratedRunFileReader;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractSorterOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final long serialVersionUID = 1L;

    protected static final int SORT_ACTIVITY_ID = 0;
    protected static final int MERGE_ACTIVITY_ID = 1;

    protected final int[] sortFields;
    protected final INormalizedKeyComputerFactory[] keyNormalizerFactories;
    protected final IBinaryComparatorFactory[] comparatorFactories;
    protected final int framesLimit;
    // Temp : limit memory - pointer arrays size is not accounted for if set to false.
    protected final boolean limitMemory;
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    //

    public AbstractSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.keyNormalizerFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        outRecDescs[0] = recordDescriptor;
        limitMemory = true;
    }

    // Temp :
    public AbstractSorterOperatorDescriptor(IOperatorDescriptorRegistry spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory[] keyNormalizerFactories, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, boolean limitMemory) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.keyNormalizerFactories = keyNormalizerFactories;
        this.comparatorFactories = comparatorFactories;
        outRecDescs[0] = recordDescriptor;
        this.limitMemory = limitMemory;
    }
    //

    public abstract SortActivity getSortActivity(ActivityId id);

    public abstract MergeActivity getMergeActivity(ActivityId id);

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = getSortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = getMergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(this, ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    public static class SortTaskState extends AbstractStateObject {
        public List<GeneratedRunFileReader> generatedRunFileReaders;
        public ISorter sorter;

        public SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    protected abstract class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        protected abstract AbstractSortRunGenerator getRunGenerator(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider) throws HyracksDataException;

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private AbstractSortRunGenerator runGen;

                @Override
                public void open() throws HyracksDataException {
                    runGen = getRunGenerator(ctx, recordDescProvider);
                    runGen.open();
                    // Temp :
                    LOGGER.log(Level.INFO, this.hashCode() + "\t" + "SortActivity::open");
                    //
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    SortTaskState state = new SortTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    runGen.close();
                    state.generatedRunFileReaders = runGen.getRuns();
                    state.sorter = runGen.getSorter();
                    if (LOGGER.isInfoEnabled()) {
                        // Temp :
                        long[] sizes = new long[runGen.getRuns().size()];
                        long totalSize = 0;
                        String sizeStr = "";
                        for (int i = 0; i < sizes.length; i++) {
                            sizes[i] = runGen.getRuns().get(i).getFileSize();
                            totalSize += sizes[i];
                            sizeStr += i + ":" + decFormat.format((double) sizes[i] / 1048576) + "\t";
                        }
                        LOGGER.log(Level.INFO,
                                this.hashCode() + "\t" + "SortActivity::close" + "\tInitialNumberOfRuns:"
                                        + runGen.getRuns().size() + "\tsize(MB):\t" + sizeStr + "\ttotal_size(MB):\t"
                                        + decFormat.format((double) totalSize / 1048576));
                        // Temp :
                        if (sizes.length == 0) {
                            state.sorter.printCurrentStatus();
                        }
                        //

                        //

                        //                        LOGGER.info("InitialNumberOfRuns:" + runGen.getRuns().size());
                    }
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    runGen.fail();
                }
            };
            return op;
        }
    }

    protected abstract class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        protected abstract AbstractExternalSortRunMerger getSortRunMerger(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, IFrameWriter writer, ISorter sorter,
                List<GeneratedRunFileReader> runs, IBinaryComparator[] comparators, INormalizedKeyComputer nmkComputer,
                int necessaryFrames);

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {

                @Override
                public void initialize() throws HyracksDataException {
                    SortTaskState state = (SortTaskState) ctx
                            .getStateObject(new TaskId(new ActivityId(getOperatorId(), SORT_ACTIVITY_ID), partition));
                    List<GeneratedRunFileReader> runs = state.generatedRunFileReaders;
                    // Temp :
                    //                    long[] sizes = new long[runs.size()];
                    //                    long totalSize = 0;
                    //                    String sizeStr = "";
                    //                    for (int i = 0; i < sizes.length; i++) {
                    //                        sizes[i] = runs.get(i).getFileSize();
                    //                        totalSize += sizes[i];
                    //                        sizeStr += i + ":" + decFormat.format((double) sizes[i] / 1048576) + "\t";
                    //                    }
                    //                    LOGGER.log(Level.INFO,
                    //                            this.hashCode() + "\t" + "MergeActivity::initialize" + "\tInitialNumberOfRuns:"
                    //                                    + runs.size() + "\tsize(MB):\t" + sizeStr + "\ttotal_size(MB):\t"
                    //                                    + decFormat.format((double) totalSize / 1048576));
                    LOGGER.log(Level.INFO, this.hashCode() + "\t" + "MergeActivity::initialize"
                            + "\tInitialNumberOfRuns:" + runs.size());

                    //
                    ISorter sorter = state.sorter;
                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; ++i) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    INormalizedKeyComputer nmkComputer = keyNormalizerFactories == null ? null
                            : keyNormalizerFactories[0].createNormalizedKeyComputer();
                    AbstractExternalSortRunMerger merger = getSortRunMerger(ctx, recordDescProvider, writer, sorter,
                            runs, comparators, nmkComputer, framesLimit);
                    merger.process();
                }
            };
            return op;
        }
    }

}
