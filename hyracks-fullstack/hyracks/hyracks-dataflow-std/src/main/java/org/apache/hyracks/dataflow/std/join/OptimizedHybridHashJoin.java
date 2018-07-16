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
package org.apache.hyracks.dataflow.std.join;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.BitSet;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.buffermanager.DeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.FramePoolBackedFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.IDeallocatableFramePool;
import org.apache.hyracks.dataflow.std.buffermanager.IPartitionedTupleBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.ISimpleFrameBufferManager;
import org.apache.hyracks.dataflow.std.buffermanager.PreferToSpillFullyOccupiedFramePolicy;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.apache.hyracks.dataflow.std.util.FrameTuplePairComparator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class mainly applies one level of HHJ on a pair of
 * relations. It is always called by the descriptor.
 */
public class OptimizedHybridHashJoin {

    // Used for special probe BigObject which can not be held into the Join memory
    private FrameTupleAppender bigProbeFrameAppender;

    enum SIDE {
        BUILD,
        PROBE
    }

    private IHyracksTaskContext ctx;

    private final String buildRelName;
    private final String probeRelName;

    private final int[] buildKeys;
    private final int[] probeKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer buildHpc;
    private final ITuplePartitionComputer probeHpc;

    private final RecordDescriptor buildRd;
    private final RecordDescriptor probeRd;

    private RunFileWriter[] buildRFWriters; //writing spilled build partitions
    private RunFileWriter[] probeRFWriters; //writing spilled probe partitions

    private final IPredicateEvaluator predEvaluator;
    private final boolean isLeftOuter;
    private final IMissingWriter[] nonMatchWriters;

    private final BitSet spilledStatus; //0=resident, 1=spilled
    private final int numOfPartitions;
    private final int memSizeInFrames;
    // Temp :
    private final boolean limitMemory;
    private final boolean hashTableGarbageCollection;
    private static final Logger LOGGER = LogManager.getLogger();
    private static final DecimalFormat decFormat = new DecimalFormat("#.######");
    //
    private InMemoryHashJoin inMemJoiner; //Used for joining resident partitions

    private IPartitionedTupleBufferManager bufferManager;
    private PreferToSpillFullyOccupiedFramePolicy spillPolicy;

    private final FrameTupleAccessor accessorBuild;
    private final FrameTupleAccessor accessorProbe;

    private IDeallocatableFramePool framePool;
    private ISimpleFrameBufferManager bufferManagerForHashTable;

    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    // stats information
    private int[] buildPSizeInTups;
    private IFrame reloadBuffer;
    private TuplePointer tempPtr = new TuplePointer(); // this is a reusable object to store the pointer,which is not used anywhere.
                                                       // we mainly use it to match the corresponding function signature.
    private int[] probePSizeInTups;

    // Original const
    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, int[] probeKeys, int[] buildKeys, IBinaryComparator[] comparators,
            RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc,
            ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval, boolean isLeftOuter,
            IMissingWriterFactory[] nullWriterFactories1) {
        this.ctx = ctx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.comparators = comparators;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.numOfPartitions = numOfPartitions;
        buildRFWriters = new RunFileWriter[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

        accessorBuild = new FrameTupleAccessor(buildRd);
        accessorProbe = new FrameTupleAccessor(probeRd);

        predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        isReversed = false;

        spilledStatus = new BitSet(numOfPartitions);

        nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }

        // Temp :
        limitMemory = true;
        hashTableGarbageCollection = true;
        //
    }

    public OptimizedHybridHashJoin(IHyracksTaskContext ctx, int memSizeInFrames, int numOfPartitions,
            String probeRelName, String buildRelName, int[] probeKeys, int[] buildKeys, IBinaryComparator[] comparators,
            RecordDescriptor probeRd, RecordDescriptor buildRd, ITuplePartitionComputer probeHpc,
            ITuplePartitionComputer buildHpc, IPredicateEvaluator predEval, boolean isLeftOuter,
            IMissingWriterFactory[] nullWriterFactories1, boolean limitMemory, boolean hashTableGarbageCollection) {
        this.ctx = ctx;
        this.memSizeInFrames = memSizeInFrames;
        this.buildRd = buildRd;
        this.probeRd = probeRd;
        this.buildHpc = buildHpc;
        this.probeHpc = probeHpc;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.comparators = comparators;
        this.buildRelName = buildRelName;
        this.probeRelName = probeRelName;

        this.numOfPartitions = numOfPartitions;
        buildRFWriters = new RunFileWriter[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

        accessorBuild = new FrameTupleAccessor(buildRd);
        accessorProbe = new FrameTupleAccessor(probeRd);

        predEvaluator = predEval;
        this.isLeftOuter = isLeftOuter;
        isReversed = false;

        spilledStatus = new BitSet(numOfPartitions);

        nonMatchWriters = isLeftOuter ? new IMissingWriter[nullWriterFactories1.length] : null;
        if (isLeftOuter) {
            for (int i = 0; i < nullWriterFactories1.length; i++) {
                nonMatchWriters[i] = nullWriterFactories1[i].createMissingWriter();
            }
        }
        // Temp :
        this.limitMemory = limitMemory;
        this.hashTableGarbageCollection = hashTableGarbageCollection;
        //
    }

    public void initBuild() throws HyracksDataException {
        framePool = new DeallocatableFramePool(ctx, memSizeInFrames * ctx.getInitialFrameSize());
        bufferManagerForHashTable = new FramePoolBackedFrameBufferManager(framePool);
        bufferManager = new VPartitionTupleBufferManager(
                PreferToSpillFullyOccupiedFramePolicy.createAtMostOneFrameForSpilledPartitionConstrain(spilledStatus),
                numOfPartitions, framePool);
        spillPolicy = new PreferToSpillFullyOccupiedFramePolicy(bufferManager, spilledStatus);
        spilledStatus.clear();
        buildPSizeInTups = new int[numOfPartitions];
        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "initBuild" + "\t#partitions:\t" + numOfPartitions + "\t#max_frames:\t"
                        + memSizeInFrames + "\tmax_size(MB):\t" + new DecimalFormat("#.######")
                                .format(((double) memSizeInFrames * ctx.getInitialFrameSize() / 1048576)));
        //
    }

    public void build(ByteBuffer buffer) throws HyracksDataException {
        accessorBuild.reset(buffer);
        int tupleCount = accessorBuild.getTupleCount();

        for (int i = 0; i < tupleCount; ++i) {
            int pid = buildHpc.partition(accessorBuild, i, numOfPartitions);
            processTuple(i, pid);
            buildPSizeInTups[pid]++;
        }

    }

    private void processTuple(int tid, int pid) throws HyracksDataException {
        while (!bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
            selectAndSpillVictim(pid);
        }
    }

    private void selectAndSpillVictim(int pid) throws HyracksDataException {
        int victimPartition = spillPolicy.selectVictimPartition(pid);
        if (victimPartition < 0) {
            throw new HyracksDataException(
                    "No more space left in the memory buffer, please assign more memory to hash-join.");
        }
        spillPartition(victimPartition);
    }

    private void spillPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.BUILD);
        bufferManager.flushPartition(pid, writer);
        bufferManager.clearPartition(pid);
        spilledStatus.set(pid);
    }

    private void closeBuildPartition(int pid) throws HyracksDataException {
        if (buildRFWriters[pid] == null) {
            throw new HyracksDataException("Tried to close the non-existing file writer.");
        }
        buildRFWriters[pid].close();
    }

    private RunFileWriter getSpillWriterOrCreateNewOneIfNotExist(int pid, SIDE whichSide) throws HyracksDataException {
        RunFileWriter[] runFileWriters = null;
        String refName = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                refName = buildRelName;
                break;
            case PROBE:
                refName = probeRelName;
                runFileWriters = probeRFWriters;
                break;
        }
        RunFileWriter writer = runFileWriters[pid];
        if (writer == null) {
            FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(refName);
            writer = new RunFileWriter(file, ctx.getIoManager());
            writer.open();
            runFileWriters[pid] = writer;
        }
        return writer;
    }

    public void closeBuild() throws HyracksDataException {
        // Flushes the remaining chunks of the all spilled partitions to the disk.
        closeAllSpilledPartitions(SIDE.BUILD);

        // Makes the space for the in-memory hash table (some partitions may need to be spilled to the disk
        // during this step in order to make the space.)
        // and tries to bring back as many spilled partitions as possible if there is free space.
        int inMemTupCount = makeSpaceForHashTableAndBringBackSpilledPartitions();

        createInMemoryJoiner(inMemTupCount);

        loadDataInMemJoin();

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "closeBuild" + "\t#in-memory_tuple-count:\t" + inMemTupCount);
        printHashTableInfo();
        //
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearBuildTempFiles() throws HyracksDataException {
        for (int i = 0; i < buildRFWriters.length; i++) {
            if (buildRFWriters[i] != null) {
                buildRFWriters[i].erase();
            }
        }
    }

    private void closeAllSpilledPartitions(SIDE whichSide) throws HyracksDataException {
        // Temp :
        int spilledCount = 0;
        int spilledMoreCount = 0;
        //
        RunFileWriter[] runFileWriters = null;
        switch (whichSide) {
            case BUILD:
                runFileWriters = buildRFWriters;
                break;
            case PROBE:
                runFileWriters = probeRFWriters;
                break;
        }
        try {
            for (int pid = spilledStatus.nextSetBit(0); pid >= 0 && pid < numOfPartitions; pid =
                    spilledStatus.nextSetBit(pid + 1)) {
                // Temp :
                spilledCount++;
                //
                if (bufferManager.getNumTuples(pid) > 0) {
                    bufferManager.flushPartition(pid, getSpillWriterOrCreateNewOneIfNotExist(pid, whichSide));
                    bufferManager.clearPartition(pid);
                    // Temp :
                    spilledMoreCount++;
                    //
                }
            }
        } finally {
            // Force to close all run file writers.
            if (runFileWriters != null) {
                for (RunFileWriter runFileWriter : runFileWriters) {
                    if (runFileWriter != null) {
                        runFileWriter.close();
                    }
                }
            }
        }

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "closeAllSpilledPartitions" + "\t#spilledPartitions:\t"
                + spilledCount + "\t#more_page_spilled_partitions:\t" + spilledMoreCount);
        //
    }

    /**
     * Makes the space for the hash table. If there is no enough space, one or more partitions will be spilled
     * to the disk until the hash table can fit into the memory. After this, bring back spilled partitions
     * if there is available memory.
     *
     * @return the number of tuples in memory after this method is executed.
     * @throws HyracksDataException
     */
    private int makeSpaceForHashTableAndBringBackSpilledPartitions() throws HyracksDataException {
        // we need number of |spilledPartitions| buffers to store the probe data
        int frameSize = ctx.getInitialFrameSize();
        long freeSpace = (long) (memSizeInFrames - spilledStatus.cardinality()) * frameSize;

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                + "\tmemSizeInFrames_count:\t" + memSizeInFrames + "\t#spilled_part:\t" + spilledStatus.cardinality()
                + "\tframesize:\t" + frameSize + "\tfreeSpace(MB):\t" + decFormat.format(freeSpace / 1048576));
        //

        // For partitions in main memory, we deduct their size from the free space.
        int inMemTupCount = 0;
        for (int p = spilledStatus.nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                spilledStatus.nextClearBit(p + 1)) {
            freeSpace -= bufferManager.getPhysicalSize(p);
            inMemTupCount += buildPSizeInTups[p];
        }

        // Calculates the expected hash table size for the given number of tuples in main memory
        // and deducts it from the free space.
        // Temp :
        long freeSpaceBeforeHashTable = freeSpace;
        //
        long hashTableByteSizeForInMemTuples =
                limitMemory ? SerializableHashTable.getExpectedTableByteSize(inMemTupCount, frameSize) : 0;
        freeSpace -= hashTableByteSizeForInMemTuples;

        // In the case where free space is less than zero after considering the hash table size,
        // we need to spill more partitions until we can accommodate the hash table in memory.
        // TODO: there may be different policies (keep spilling minimum, spilling maximum, find a similar size to the
        //                                        hash table, or keep spilling from the first partition)
        boolean moreSpilled = false;

        // Temp :
        LOGGER.log(Level.INFO,
                this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                        + "\tfree_space_before_hash_table(MB):\t"
                        + decFormat.format((double) freeSpaceBeforeHashTable / 1048576) + "\tfreeSpace(MB):\t"
                        + decFormat.format((double) freeSpace / 1048576) + "\thash_table_size(MB):\t"
                        + decFormat.format((double) hashTableByteSizeForInMemTuples / 1048576) + "\tinMemTupCount:\t"
                        + inMemTupCount + "\tlimitMemory:\t" + limitMemory);
        //

        // No space to accommodate the hash table? Then, we spill one or more partitions to the disk.
        if (freeSpace < 0) {
            // Tries to find a best-fit partition not to spill many partitions.
            int pidToSpill = selectSinglePartitionToSpill(freeSpace, inMemTupCount, frameSize);
            if (pidToSpill >= 0) {
                // There is a suitable one. We spill that partition to the disk.
                long hashTableSizeDecrease =
                        limitMemory ? -SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(inMemTupCount,
                                -buildPSizeInTups[pidToSpill], frameSize) : 0;
                // Temp :
                int physicalSize = bufferManager.getPhysicalSize(pidToSpill);
                int tupleCount = buildPSizeInTups[pidToSpill];
                //
                freeSpace = freeSpace + physicalSize + hashTableSizeDecrease;
                inMemTupCount -= tupleCount;
                spillPartition(pidToSpill);
                closeBuildPartition(pidToSpill);
                moreSpilled = true;

                // Temp :
                LOGGER.log(Level.INFO, this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                        + "\tfound a suitable partiton to spill - partition ID:\t" + pidToSpill + "\tsize(MB):\t"
                        + decFormat.format((double) physicalSize / 1048576) + "\t#tuple:\t" + tupleCount
                        + "\thashTableSizeDecrease(MB):\t" + decFormat.format((double) hashTableSizeDecrease / 1048576)
                        + "\tnew_freeSpace:\t" + freeSpace + "\tinMemTupleCount:\t" + inMemTupCount);
                //

            } else {
                // There is no single suitable partition. So, we need to spill multiple partitions to the disk
                // in order to accommodate the hash table.

                // Temp :
                int spilledPartCount = 0;
                int spilledTupleCount = 0;
                int spilledPartSize = 0;
                //

                for (int p = spilledStatus.nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                        spilledStatus.nextClearBit(p + 1)) {
                    int spaceToBeReturned = bufferManager.getPhysicalSize(p);
                    int numberOfTuplesToBeSpilled = buildPSizeInTups[p];
                    if (spaceToBeReturned == 0 || numberOfTuplesToBeSpilled == 0) {
                        continue;
                    }
                    spillPartition(p);
                    closeBuildPartition(p);
                    moreSpilled = true;
                    // Since the number of tuples in memory has been decreased,
                    // the hash table size will be decreased, too.
                    // We put minus since the method returns a negative value to represent a newly reclaimed space.
                    long expectedHashTableSizeDecrease =
                            limitMemory ? -SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(inMemTupCount,
                                    -numberOfTuplesToBeSpilled, frameSize) : 0;
                    freeSpace = freeSpace + spaceToBeReturned + expectedHashTableSizeDecrease;
                    // Adjusts the hash table size
                    inMemTupCount -= numberOfTuplesToBeSpilled;

                    // Temp :
                    spilledPartCount++;
                    spilledTupleCount = spilledTupleCount + numberOfTuplesToBeSpilled;
                    spilledPartSize = spilledPartSize + spaceToBeReturned;
                    //
                    if (freeSpace >= 0) {
                        // Temp :
                        LOGGER.log(Level.INFO,
                                this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                                        + "\tfound no suitable partiton to spill, thus spilled many partitions - #spilled_partition_count:\t"
                                        + spilledPartCount + "\tsize(MB):\t"
                                        + decFormat.format((double) spilledPartSize / 1048576)
                                        + "\t#spilled_tuple_count:\t" + spilledTupleCount);
                        //
                        break;
                    }
                }
            }
        }

        // If more partitions have been spilled to the disk, calculate the expected hash table size again
        // before bringing some partitions to main memory.
        if (moreSpilled) {
            hashTableByteSizeForInMemTuples = SerializableHashTable.getExpectedTableByteSize(inMemTupCount, frameSize);
            // Temp :
            LOGGER.log(Level.INFO,
                    this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                            + "\thash_table_size_after_spilling_partitions:\t"
                            + decFormat.format((double) hashTableByteSizeForInMemTuples / 1048576));
            //
        }

        // Brings back some partitions if there is enough free space.
        int pid = 0;
        // Temp :
        int broughtBackPartCount = 0;
        int broughtBackPartSize = 0;
        int broughtBackTupleCount = 0;
        //
        while ((pid = selectPartitionsToReload(freeSpace, pid, inMemTupCount)) >= 0) {
            if (!loadSpilledPartitionToMem(pid, buildRFWriters[pid])) {
                break;
            }
            long expectedHashTableByteSizeIncrease = limitMemory ? SerializableHashTable
                    .calculateByteSizeDeltaForTableSizeChange(inMemTupCount, buildPSizeInTups[pid], frameSize) : 0;
            freeSpace = freeSpace - bufferManager.getPhysicalSize(pid) - expectedHashTableByteSizeIncrease;
            inMemTupCount += buildPSizeInTups[pid];
            // Adjusts the hash table size
            hashTableByteSizeForInMemTuples += expectedHashTableByteSizeIncrease;
            // Temp :
            broughtBackPartCount++;
            broughtBackPartSize = broughtBackPartSize + bufferManager.getPhysicalSize(pid);
            broughtBackTupleCount = broughtBackTupleCount + buildPSizeInTups[pid];
            //
        }

        // Temp :
        if (broughtBackPartCount > 0) {
            // Temp :
            LOGGER.log(Level.INFO,
                    this.hashCode() + "\t" + "makeSpaceForHashTableAndBringBackSpilledPartitions"
                            + "\tbrought back some partitions - #brought_back_partition_count:\t" + broughtBackPartCount
                            + "\tsize(MB):\t" + decFormat.format((double) broughtBackPartSize / 1048576)
                            + "\t#brought_back_tuple_count:\t" + broughtBackTupleCount);
            //
        }
        //

        return inMemTupCount;
    }

    /**
     * Finds a best-fit partition that will be spilled to the disk to make enough space to accommodate the hash table.
     *
     * @return the partition id that will be spilled to the disk. Returns -1 if there is no single suitable partition.
     */
    private int selectSinglePartitionToSpill(long currentFreeSpace, int currentInMemTupCount, int frameSize) {
        long spaceAfterSpill;
        long minSpaceAfterSpill = (long) memSizeInFrames * frameSize;
        int minSpaceAfterSpillPartID = -1;

        for (int p = spilledStatus.nextClearBit(0); p >= 0 && p < numOfPartitions; p =
                spilledStatus.nextClearBit(p + 1)) {
            if (buildPSizeInTups[p] == 0 || bufferManager.getPhysicalSize(p) == 0) {
                continue;
            }
            // We put minus since the method returns a negative value to represent a newly reclaimed space.
            spaceAfterSpill = currentFreeSpace + bufferManager.getPhysicalSize(p);
            spaceAfterSpill = limitMemory ? spaceAfterSpill + (-SerializableHashTable
                    .calculateByteSizeDeltaForTableSizeChange(currentInMemTupCount, -buildPSizeInTups[p], frameSize))
                    : 0;
            if (spaceAfterSpill == 0) {
                // Found the perfect one. Just returns this partition.
                return p;
            } else if (spaceAfterSpill > 0 && spaceAfterSpill < minSpaceAfterSpill) {
                // We want to find the best-fit partition to avoid many partition spills.
                minSpaceAfterSpill = spaceAfterSpill;
                minSpaceAfterSpillPartID = p;
            }
        }
        return minSpaceAfterSpillPartID;
    }

    private int selectPartitionsToReload(long freeSpace, int pid, int inMemTupCount) {
        for (int i = spilledStatus.nextSetBit(pid); i >= 0 && i < numOfPartitions; i =
                spilledStatus.nextSetBit(i + 1)) {
            int spilledTupleCount = buildPSizeInTups[i];
            // Expected hash table size increase after reloading this partition
            long expectedHashTableByteSizeIncrease =
                    limitMemory ? SerializableHashTable.calculateByteSizeDeltaForTableSizeChange(inMemTupCount,
                            spilledTupleCount, ctx.getInitialFrameSize()) : 0;
            if (freeSpace >= buildRFWriters[i].getFileSize() + expectedHashTableByteSizeIncrease) {
                return i;
            }
        }
        return -1;
    }

    private boolean loadSpilledPartitionToMem(int pid, RunFileWriter wr) throws HyracksDataException {
        RunFileReader r = wr.createReader();
        try {
            r.open();
            if (reloadBuffer == null) {
                reloadBuffer = new VSizeFrame(ctx);
            }
            while (r.nextFrame(reloadBuffer)) {
                accessorBuild.reset(reloadBuffer.getBuffer());
                for (int tid = 0; tid < accessorBuild.getTupleCount(); tid++) {
                    if (bufferManager.insertTuple(pid, accessorBuild, tid, tempPtr)) {
                        continue;
                    }
                    // for some reason (e.g. due to fragmentation) if the inserting failed,
                    // we need to clear the occupied frames
                    bufferManager.clearPartition(pid);
                    return false;
                }
            }
            // Closes and deletes the run file if it is already loaded into memory.
            r.setDeleteAfterClose(true);
        } finally {
            r.close();
        }
        spilledStatus.set(pid, false);
        buildRFWriters[pid] = null;
        return true;
    }

    private void createInMemoryJoiner(int inMemTupCount) throws HyracksDataException {
        ISerializableTable table = new SerializableHashTable(inMemTupCount, ctx, bufferManagerForHashTable, limitMemory,
                hashTableGarbageCollection);
        inMemJoiner = new InMemoryHashJoin(ctx, new FrameTupleAccessor(probeRd), probeHpc,
                new FrameTupleAccessor(buildRd), buildRd, buildHpc,
                new FrameTuplePairComparator(probeKeys, buildKeys, comparators), isLeftOuter, nonMatchWriters, table,
                predEvaluator, isReversed, bufferManagerForHashTable, limitMemory, hashTableGarbageCollection);
        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "createInMemoryJoiner" + "\tlimitMemory:\t" + limitMemory
                + "\tin_memory_tuple_count:\t" + inMemTupCount);
        //
    }

    private void loadDataInMemJoin() throws HyracksDataException {

        for (int pid = 0; pid < numOfPartitions; pid++) {
            if (!spilledStatus.get(pid)) {
                bufferManager.flushPartition(pid, new IFrameWriter() {
                    @Override
                    public void open() throws HyracksDataException {

                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        inMemJoiner.build(buffer);
                    }

                    @Override
                    public void fail() throws HyracksDataException {

                    }

                    @Override
                    public void close() throws HyracksDataException {

                    }
                });
            }
        }

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "loadDataInMemJoin\t" + "finish loading the data\n");
        //
    }

    public void initProbe() throws HyracksDataException {

        probePSizeInTups = new int[numOfPartitions];
        probeRFWriters = new RunFileWriter[numOfPartitions];

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "initProbe");
        //

    }

    public void probe(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        accessorProbe.reset(buffer);
        int tupleCount = accessorProbe.getTupleCount();

        if (isBuildRelAllInMemory()) {
            inMemJoiner.join(buffer, writer);
            return;
        }
        inMemJoiner.resetAccessorProbe(accessorProbe);
        for (int i = 0; i < tupleCount; ++i) {
            int pid = probeHpc.partition(accessorProbe, i, numOfPartitions);

            if (buildPSizeInTups[pid] > 0 || isLeftOuter) { //Tuple has potential match from previous phase
                if (spilledStatus.get(pid)) { //pid is Spilled
                    while (!bufferManager.insertTuple(pid, accessorProbe, i, tempPtr)) {
                        int victim = pid;
                        if (bufferManager.getNumTuples(pid) == 0) { // current pid is empty, choose the biggest one
                            victim = spillPolicy.findSpilledPartitionWithMaxMemoryUsage();
                        }
                        if (victim < 0) { // current tuple is too big for all the free space
                            flushBigProbeObjectToDisk(pid, accessorProbe, i);
                            break;
                        }
                        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(victim, SIDE.PROBE);
                        bufferManager.flushPartition(victim, runFileWriter);
                        bufferManager.clearPartition(victim);
                    }
                } else { //pid is Resident
                    inMemJoiner.join(i, writer);
                }
                probePSizeInTups[pid]++;
            }
        }
    }

    private void flushBigProbeObjectToDisk(int pid, FrameTupleAccessor accessorProbe, int i)
            throws HyracksDataException {
        if (bigProbeFrameAppender == null) {
            bigProbeFrameAppender = new FrameTupleAppender(new VSizeFrame(ctx));
        }
        RunFileWriter runFileWriter = getSpillWriterOrCreateNewOneIfNotExist(pid, SIDE.PROBE);
        if (!bigProbeFrameAppender.append(accessorProbe, i)) {
            throw new HyracksDataException("The given tuple is too big");
        }
        bigProbeFrameAppender.write(runFileWriter, true);
        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "flushBigProbeObjectToDisk" + "\tpartition id:\t" + pid);
        //

    }

    private boolean isBuildRelAllInMemory() {
        return spilledStatus.nextSetBit(0) < 0;
    }

    public void completeProbe(IFrameWriter writer) throws HyracksDataException {
        //We do NOT join the spilled partitions here, that decision is made at the descriptor level
        //(which join technique to use)
        inMemJoiner.completeJoin(writer);

        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "completeProbe");
        //
    }

    public void releaseResource() throws HyracksDataException {
        // Temp :
        LOGGER.log(Level.INFO, this.hashCode() + "\t" + "releaseResource");
        //        inMemJoiner.printTableInfo();
        //
        inMemJoiner.closeTable();
        closeAllSpilledPartitions(SIDE.PROBE);
        bufferManager.close();
        inMemJoiner = null;
        bufferManager = null;
        bufferManagerForHashTable = null;
    }

    /**
     * In case of failure happens, we need to clear up the generated temporary files.
     */
    public void clearProbeTempFiles() throws HyracksDataException {
        for (int i = 0; i < probeRFWriters.length; i++) {
            if (probeRFWriters[i] != null) {
                probeRFWriters[i].erase();
            }
        }
    }

    public RunFileReader getBuildRFReader(int pid) throws HyracksDataException {
        return ((buildRFWriters[pid] == null) ? null : (buildRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getBuildPartitionSizeInTup(int pid) {
        return (buildPSizeInTups[pid]);
    }

    public RunFileReader getProbeRFReader(int pid) throws HyracksDataException {
        return ((probeRFWriters[pid] == null) ? null : (probeRFWriters[pid]).createDeleteOnCloseReader());
    }

    public int getProbePartitionSizeInTup(int pid) {
        return (probePSizeInTups[pid]);
    }

    public int getMaxBuildPartitionSize() {
        int max = buildPSizeInTups[0];
        for (int i = 1; i < buildPSizeInTups.length; i++) {
            if (buildPSizeInTups[i] > max) {
                max = buildPSizeInTups[i];
            }
        }
        return max;
    }

    public int getMaxProbePartitionSize() {
        int max = probePSizeInTups[0];
        for (int i = 1; i < probePSizeInTups.length; i++) {
            if (probePSizeInTups[i] > max) {
                max = probePSizeInTups[i];
            }
        }
        return max;
    }

    public BitSet getPartitionStatus() {
        return spilledStatus;
    }

    public void setIsReversed(boolean b) {
        isReversed = b;
    }

    /**
     * Prints out the detailed information for partitions: in-memory and spilled partitions.
     * This method exists for a debug purpose.
     */
    public String printPartitionInfo(SIDE whichSide) {
        // Temp :
        DecimalFormat decFormat = new DecimalFormat("#.######");
        //
        StringBuilder buf = new StringBuilder();
        buf.append(">>> " + this + " " + Thread.currentThread().getId() + " printInfo():" + "\n");
        if (whichSide == SIDE.BUILD) {
            buf.append("BUILD side" + "\n");
        } else {
            buf.append("PROBE side" + "\n");
        }
        buf.append("# of partitions:\t" + numOfPartitions + "\t#spilled:\t" + spilledStatus.cardinality()
                + "\t#in-memory:\t" + (numOfPartitions - spilledStatus.cardinality()) + "\n");
        buf.append("(A) Spilled partitions" + "\n");
        int spilledTupleCount = 0;
        int spilledPartByteSize = 0;
        for (int pid = spilledStatus.nextSetBit(0); pid >= 0 && pid < numOfPartitions; pid =
                spilledStatus.nextSetBit(pid + 1)) {
            if (whichSide == SIDE.BUILD) {
                spilledTupleCount += buildPSizeInTups[pid];
                spilledPartByteSize += buildRFWriters[pid].getFileSize();
                buf.append("part:\t" + pid + "\t#tuple:\t" + buildPSizeInTups[pid] + "\tsize(MB):\t"
                        + decFormat.format(((double) buildRFWriters[pid].getFileSize() / 1048576)) + "\n");
            } else {
                spilledTupleCount += probePSizeInTups[pid];
                spilledPartByteSize += probeRFWriters[pid].getFileSize();
            }
        }
        if (spilledStatus.cardinality() > 0) {
            buf.append("# of spilled tuples:\t" + spilledTupleCount + "\tsize(MB):\t"
                    + ((double) spilledPartByteSize / 1048576) + "\tavg #tuples per spilled part:\t"
                    + ((double) spilledTupleCount / spilledStatus.cardinality()) + "\tavg size per part(MB):\t"
                    + ((double) spilledPartByteSize / 1048576 / spilledStatus.cardinality()) + "\n");
        }
        buf.append("(B) In-memory partitions" + "\n");
        int inMemoryTupleCount = 0;
        int inMemoryPartByteSize = 0;
        int inMemoryPartCount = 0;
        for (int pid = spilledStatus.nextClearBit(0); pid >= 0 && pid < numOfPartitions; pid =
                spilledStatus.nextClearBit(pid + 1)) {
            inMemoryPartCount++;
            if (whichSide == SIDE.BUILD) {
                inMemoryTupleCount += buildPSizeInTups[pid];
                inMemoryPartByteSize += bufferManager.getPhysicalSize(pid);
            } else {
                inMemoryTupleCount += probePSizeInTups[pid];
                inMemoryPartByteSize += bufferManager.getPhysicalSize(pid);
            }
        }
        buf.append("# of in-memory tuples:\t" + inMemoryTupleCount + "\tsize(MB):\t"
                + decFormat.format(((double) inMemoryPartByteSize / 1048576)) + "\tavg #tuples per in-memory part:\t"
                + decFormat.format(((double) inMemoryTupleCount / inMemoryPartCount)) + "\tavg size per part(MB):\t"
                + decFormat.format(((double) inMemoryPartByteSize / 1048576 / inMemoryPartCount)) + "\n");
        if (inMemoryTupleCount + spilledTupleCount > 0) {
            buf.append("# of all tuples:\t" + (inMemoryTupleCount + spilledTupleCount) + "\tsize(MB):\t"
                    + decFormat.format(((double) (inMemoryPartByteSize + spilledPartByteSize) / 1048576))
                    + "\tratio of spilled tuples:\t"
                    + decFormat.format(((double) spilledTupleCount / (inMemoryTupleCount + spilledTupleCount))) + "\n");
        } else {
            buf.append("# of all tuples:\t" + (inMemoryTupleCount + spilledTupleCount) + "\tsize(MB):\t"
                    + decFormat.format(((double) (inMemoryPartByteSize + spilledPartByteSize) / 1048576))
                    + "\tratio of spilled tuples:\t" + "N/A" + "\n");
        }
        return buf.toString();
    }

    public void printHashTableInfo() {
        if (inMemJoiner != null) {
            inMemJoiner.printTableInfo();
        }
    }
}
