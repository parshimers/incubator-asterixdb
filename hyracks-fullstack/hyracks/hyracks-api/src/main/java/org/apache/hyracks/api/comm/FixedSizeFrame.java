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

package org.apache.hyracks.api.comm;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FixedSizeFrame implements IFrame {

    private ByteBuffer buffer;

    public FixedSizeFrame() {

    }

    public FixedSizeFrame(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void ensureFrameSize(int frameSize) throws HyracksDataException {
        throw new HyracksDataException("FixedSizeFrame doesn't support capacity changes");
    }

    @Override
    public void resize(int frameSize) throws HyracksDataException {
        throw new HyracksDataException("FixedSizeFrame doesn't support capacity changes");
    }

    @Override
    public int getFrameSize() {
        return buffer.capacity();
    }

    @Override
    public int getMinSize() {
        return buffer.capacity() / FrameHelper.deserializeNumOfMinFrame(buffer, 0);
    }

    @Override
    public void reset() throws HyracksDataException {
        buffer.clear();
    }
}
