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

package edu.uci.ics.asterix.common.ioopcallbacks;

import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponent;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.LSMOperationType;

// A single LSMIOOperationCallback per LSM index used to perform actions around Flush and Merge operations
public abstract class AbstractLSMIOOperationCallback implements ILSMIOOperationCallback {

    // First LSN per mutable component
    protected long[] firstLSNs;
    // A boolean array to keep track of flush operations
    protected boolean[] flushRequested;
    // I think this was meant to be mutableLastLSNs
    // protected long[] immutableLastLSNs;
    protected long[] mutableLastLSNs;
    // Index of the currently flushing or next to be flushed component
    protected int readIndex;
    // Index of the currently being written to component
    protected int writeIndex;

    @Override
    public void setNumOfMutableComponents(int count) {
        mutableLastLSNs = new long[count];
        firstLSNs = new long[count];
        flushRequested = new boolean[count];
        readIndex = 0;
        writeIndex = 0;
    }

    @Override
    public void beforeOperation(LSMOperationType opType) {
        if (opType == LSMOperationType.FLUSH) {
            /*
             * This method was called on the scheduleFlush operation.
             * We set the lastLSN to the last LSN for the index (the LSN for the flush log)
             * We mark the component flushing flag
             * We then move the write pointer to the next component and sets its first LSN to the flush log LSN
             */
            synchronized (this) {
                flushRequested[writeIndex] = true;
                writeIndex = (writeIndex + 1) % mutableLastLSNs.length;
                // Set the firstLSN of the next component unless it is being flushed
                if (writeIndex != readIndex) {
                    firstLSNs[writeIndex] = mutableLastLSNs[writeIndex];
                }
            }
        }
    }

    @Override
    public void afterFinalize(LSMOperationType opType, ILSMComponent newComponent) {
        // The operation was complete and the next I/O operation for the LSM index didn't start yet
        if (opType == LSMOperationType.FLUSH && newComponent != null) {
            synchronized (this) {
                flushRequested[readIndex] = false;
                // if the component which just finished flushing is the component that will be modified next, we set its first LSN to its previous LSN
                if (readIndex == writeIndex) {
                    firstLSNs[writeIndex] = mutableLastLSNs[writeIndex];
                }
                readIndex = (readIndex + 1) % mutableLastLSNs.length;
            }
        }
    }

    public abstract long getComponentLSN(List<ILSMComponent> oldComponents) throws HyracksDataException;

    protected void putLSNIntoMetadata(ITreeIndex treeIndex, List<ILSMComponent> oldComponents)
            throws HyracksDataException {
        long componentLSN = getComponentLSN(oldComponents);
        treeIndex.getMetaManager().setLSN(componentLSN);
    }

    protected long getTreeIndexLSN(ITreeIndex treeIndex) throws HyracksDataException {
        return treeIndex.getMetaManager().getLSN();
    }

    public void updateLastLSN(long lastLSN) {
        mutableLastLSNs[writeIndex] = lastLSN;
    }

    public void setFirstLSN(long firstLSN) throws AsterixException {
        // We make sure that this method is only called on an empty component so the first LSN is not set incorrectly
        firstLSNs[writeIndex] = firstLSN;
    }

    public synchronized long getFirstLSN() {
        // We make sure that this method is only called on a non-empty component so the returned LSN is meaningful
        // The firstLSN is always the lsn of the currently being flushed component or the next to be flushed when no flush operation is on going
        return firstLSNs[readIndex];
    }

    public synchronized boolean hasPendingFlush() {

        for (int i = 0; i < flushRequested.length; i++) {
            if (flushRequested[i]) {
                return true;
            }
        }
        return false;
    }
}
