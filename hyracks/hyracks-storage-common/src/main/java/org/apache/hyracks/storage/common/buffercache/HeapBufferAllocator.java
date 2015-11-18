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
package org.apache.hyracks.storage.common.buffercache;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class HeapBufferAllocator implements ICacheMemoryAllocator {
    @Override
    public ByteBuffer[] allocate(int pageSize, int numPages) {
        ByteBuffer[] buffers = new ByteBuffer[numPages];
        for (int i = 0; i < numPages; ++i) {
            buffers[i] = ByteBuffer.allocate(pageSize);
        }
        return buffers;
    }

    @Override
    public ByteBuffer[] ensureAvailabilityThenAllocate(int pageSize, int numPages) throws HyracksDataException {
        return allocate(pageSize, numPages);
    }
}