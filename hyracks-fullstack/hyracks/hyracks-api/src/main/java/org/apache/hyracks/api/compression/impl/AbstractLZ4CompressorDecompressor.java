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
package org.apache.hyracks.api.compression.impl;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

abstract class AbstractLZ4CompressorDecompressor implements ICompressorDecompressor {

    protected static final LZ4Factory FACTORY = LZ4Factory.fastestInstance();

    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    protected AbstractLZ4CompressorDecompressor(LZ4Compressor compressor, LZ4FastDecompressor decompressor) {
        this.compressor = compressor;
        this.decompressor = decompressor;
    }

    @Override
    public int computeCompressBufferSize(int uncompressedBufferSize) {
        return compressor.maxCompressedLength(uncompressedBufferSize);
    }

    @Override
    public int compress(byte[] uncompressedBuffer, int uOffset, int uLength, byte[] compressedBuffer, int cOffset)
            throws HyracksDataException {
        try {
            return compressor.compress(uncompressedBuffer, uOffset, uLength, compressedBuffer, cOffset);
        } catch (LZ4Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public int uncompress(byte[] compressedBuffer, int cOffset, int cLength, byte[] uncompressedBuffer, int uOffset,
            int uLength) throws HyracksDataException {
        try {
            //LZ4 decompress function is different. It returns the compressed size.
            decompressor.decompress(compressedBuffer, cOffset, uncompressedBuffer, uOffset, uLength);
            //Return the uncompressed size
            return uLength;
        } catch (LZ4Exception e) {
            throw HyracksDataException.create(e);
        }
    }

}
