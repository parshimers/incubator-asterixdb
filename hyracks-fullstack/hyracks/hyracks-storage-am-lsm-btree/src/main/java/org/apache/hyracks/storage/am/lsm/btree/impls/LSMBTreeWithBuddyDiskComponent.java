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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IFileHandle;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import org.apache.hyracks.storage.am.btree.impls.BTree;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractDiskLSMComponent;

public class LSMBTreeWithBuddyDiskComponent extends AbstractDiskLSMComponent {

    private final BTree btree;
    private final BTree buddyBtree;
    private final BloomFilter bloomFilter;

    public LSMBTreeWithBuddyDiskComponent(BTree btree, BTree buddyBtree, BloomFilter bloomFilter) {
        this.btree = btree;
        this.buddyBtree = buddyBtree;
        this.bloomFilter = bloomFilter;
    }

    @Override
    protected void destroy() throws HyracksDataException {
        btree.deactivate();
        btree.destroy();
        buddyBtree.deactivate();
        buddyBtree.destroy();
        bloomFilter.deactivate();
        bloomFilter.destroy();
    }

    public BTree getBTree() {
        return btree;
    }

    public BTree getBuddyBTree() {
        return buddyBtree;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public long getComponentSize() {
//        long size = btree.getFileReference().getFile().length();
//        size += buddyBtree.getFileReference().getFile().length();
//        size += bloomFilter.getFileReference().getFile().length();
//        return size;
        IIOManager iomanager = btree.getBufferCache().getIOManager();
        long btreeSize = 0, buddyBTreeSize =0, bloomSize = 0;
        try {
            IFileHandle btreeHandle = iomanager.open(btree.getFileReference(),
                    IIOManager.FileReadWriteMode.READ_ONLY,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);

            IFileHandle buddyBtreeHandle = iomanager.open(buddyBtree.getFileReference(),
                    IIOManager.FileReadWriteMode.READ_ONLY,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);

            IFileHandle bloomHandle = iomanager.open(bloomFilter.getFileReference(),
                    IIOManager.FileReadWriteMode.READ_ONLY,
                    IIOManager.FileSyncMode.METADATA_ASYNC_DATA_ASYNC);

            btreeSize = iomanager.getSize(btreeHandle);
            buddyBTreeSize = iomanager.getSize(btreeHandle);
            bloomSize = iomanager.getSize(bloomHandle);
        } catch (HyracksDataException e) {
            btreeSize = -1;
        }
        return btreeSize + buddyBTreeSize + bloomSize ;
    }

    @Override
    public int getFileReferenceCount() {
        return btree.getBufferCache().getFileReferenceCount(btree.getFileId());
    }

}
