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
package org.apache.asterix.common.storage;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;

/**
 * An interface for the compression manager which handles all the registered
 * schemes and validate the provided configurations.
 */
@FunctionalInterface
public interface ICompressionManager {

    /**
     * Get Compressor/Decompressor factory
     *
     * @param hints
     *            DDL hints for the dataset
     * @param isPrimary
     *            Is primary index
     * @return
     *         Compressor/Decompressor if scheme is specified or null
     * @throws CompilationException
     */
    public ICompressorDecompressorFactory getFactory(String schemeName, boolean isPrimary) throws CompilationException;
}
