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

package org.apache.hyracks.storage.am.common.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

// all meta pages of this kind have a negative level
// the first meta page has level -1, all other meta pages have level -2
// the first meta page is special because it guarantees to have a correct max page
// other meta pages (i.e., with level -2) have junk in the max page field

public class LIFOMetaDataFrame implements ITreeIndexMetaDataFrame {

    // Arbitrarily chosen magic integer.
    protected static final int MAGIC_VALID_INT = 0x1B16DA7A;

    protected static final int TUPLE_COUNT_OFF = 0; //0
    protected static final int FREE_SPACE_OFF = TUPLE_COUNT_OFF + 4; //4
    protected static final int MAX_PAGE_OFF = FREE_SPACE_OFF + 4; //8
    protected static final int LEVEL_OFF = MAX_PAGE_OFF + 12; //20
    protected static final int NEXT_PAGE_OFF = LEVEL_OFF + 1; // 21
    protected static final int VALID_OFF = NEXT_PAGE_OFF + 4; // 25

    // The ADDITIONAL_FILTERING_PAGE_OFF is used only for LSM indexes.
    // We store the page id that will be used to store the information of the the filter that is associated with a disk component.
    // It is only set in the first meta page other meta pages (i.e., with level -2) have junk in the max page field.
    private static final int ADDITIONAL_FILTERING_PAGE_OFF = VALID_OFF + 4; // 29
    public static final int LSN_OFF = ADDITIONAL_FILTERING_PAGE_OFF + 4; // 33
    public static final int STORAGE_VERSION_OFF = LSN_OFF + 8; //41

    protected ICachedPage page = null;
    protected ByteBuffer buf = null;

    public int getMaxPage() {
        return buf.getInt(MAX_PAGE_OFF);
    }

    public void setMaxPage(int maxPage) {
        buf.putInt(MAX_PAGE_OFF, maxPage);
    }

    public int getFreePage() {
        int tupleCount = buf.getInt(TUPLE_COUNT_OFF);
        if (tupleCount > 0) {
            // return the last page from the linked list of free pages
            // TODO: this is a dumb policy, but good enough for now
            int lastPageOff = buf.getInt(FREE_SPACE_OFF) - 4;
            buf.putInt(FREE_SPACE_OFF, lastPageOff);
            buf.putInt(TUPLE_COUNT_OFF, tupleCount - 1);
            return buf.getInt(lastPageOff);
        } else {
            return -1;
        }
    }

    // must be checked before adding free page
    // user of this class is responsible for getting a free page as a new meta
    // page, latching it, etc. if there is no space on this page
    public boolean hasSpace() {
        return buf.getInt(FREE_SPACE_OFF) + 4 < buf.capacity();
    }

    // no bounds checking is done, there must be free space
    public void addFreePage(int freePage) {
        int freeSpace = buf.getInt(FREE_SPACE_OFF);
        buf.putInt(freeSpace, freePage);
        buf.putInt(FREE_SPACE_OFF, freeSpace + 4);
        buf.putInt(TUPLE_COUNT_OFF, buf.getInt(TUPLE_COUNT_OFF) + 1);
    }

    @Override
    public byte getLevel() {
        return buf.get(LEVEL_OFF);
    }

    @Override
    public void setLevel(byte level) {
        buf.put(LEVEL_OFF, level);
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
        this.buf = page.getBuffer();
    }

    @Override
    public void initBuffer(byte level) {
        buf.putInt(TUPLE_COUNT_OFF, 0);
        buf.putInt(FREE_SPACE_OFF, LSN_OFF + 8);
        buf.putInt(MAX_PAGE_OFF, 0);
        buf.put(LEVEL_OFF, level);
        buf.putInt(NEXT_PAGE_OFF, -1);
        buf.putInt(ADDITIONAL_FILTERING_PAGE_OFF, -1);
        buf.putInt(STORAGE_VERSION_OFF, VERSION);
        setValid(false);
    }

    @Override
    public int getNextPage() {
        return buf.getInt(NEXT_PAGE_OFF);
    }

    @Override
    public void setNextPage(int nextPage) {
        buf.putInt(NEXT_PAGE_OFF, nextPage);
    }

    @Override
    public boolean isValid() {
        return buf.getInt(VALID_OFF) == MAGIC_VALID_INT;
    }

    @Override
    public void setValid(boolean isValid) {
        if (isValid) {
            buf.putInt(VALID_OFF, MAGIC_VALID_INT);
        } else {
            buf.putInt(VALID_OFF, 0);
        }
    }

    @Override
    public long getLSN() {
        return buf.getLong(LSN_OFF);
    }

    @Override
    public void setLSN(long lsn) {
        buf.putLong(LSN_OFF, lsn);
    }

    @Override
    public int getVersion() {
        return buf.getInt(STORAGE_VERSION_OFF);
    }

    @Override
    public int getLSMComponentFilterPageId() {
        return buf.getInt(ADDITIONAL_FILTERING_PAGE_OFF);
    }

    @Override
    public void setLSMComponentFilterPageId(int filterPage) {
        buf.putInt(ADDITIONAL_FILTERING_PAGE_OFF, filterPage);
    }
}
