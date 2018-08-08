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
package org.apache.hyracks.api.compression;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An API for block compressor/decompressor.
 *
 * Note: Should never allocate any buffer in compress/uncompress operations and it must be stateless to be thread safe.
 */
public interface ICompressorDecompressor {
    /**
     * Computes the required buffer size for <i>compress()</i>.
     *
     * @param uncompressedBufferSize
     *            The size of the uncompressed buffer.
     * @return
     *         The required buffer size
     */
    public int computeCompressBufferSize(int uncompressedBufferSize);

    /**
     * Compress <i>uncompressedBuffer</i> into <i>compressedBuffer</i>
     *
     * @param uncompressedBuffer
     *            Uncompressed source block
     * @param uOffset
     *            uncompressed block offset
     * @param uLength
     *            uncompressed block length
     * @param compressedBuffer
     *            compressed destination block
     * @param cOffset
     *            compressed block offset
     * @param cLength
     *            compressed block Length
     * @return Size after compression
     * @throws HyracksDataException
     *             An exception will be thrown if the <i>compressedBuffer</i> size is not sufficient.
     */
    public int compress(byte[] uncompressedBuffer, int uOffset, int uLength, byte[] compressedBuffer, int cOffset)
            throws HyracksDataException;

    /**
     * Uncompress <i>compressedBuffer</i> into <i>uncompressedBuffer</i>
     *
     * @param compressedBuffer
     *            compressed source block
     * @param cOffset
     *            compressed block offset
     * @param cLength
     *            compressed block length
     * @param uncompressedBuffer
     *            uncompressed destination block
     * @param uOffset
     *            uncompressed block offset
     * @param uLength
     *            uncompressed block length
     * @return Size after decompression
     * @throws HyracksDataException
     *             An exception will be thrown if the <i>uncompressedBuffer</i> size is not sufficient.
     */
    public int uncompress(byte[] compressedBuffer, int cOffset, int cLength, byte[] uncompressedBuffer, int uOffset,
            int length) throws HyracksDataException;
}
