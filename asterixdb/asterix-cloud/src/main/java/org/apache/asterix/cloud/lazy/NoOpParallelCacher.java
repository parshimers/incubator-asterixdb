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
package org.apache.asterix.cloud.lazy;

import java.util.Collection;

import org.apache.hyracks.api.io.FileReference;

public class NoOpParallelCacher implements IParallelCacher {
    public static final IParallelCacher INSTANCE = new NoOpParallelCacher();

    @Override
    public boolean isCached(FileReference indexDir) {
        return false;
    }

    @Override
    public boolean downloadData(FileReference indexFile) {
        return false;
    }

    @Override
    public boolean downloadMetadata(FileReference indexFile) {
        return false;
    }

    @Override
    public boolean remove(Collection<FileReference> deletedFiles) {
        return false;
    }

    @Override
    public boolean remove(FileReference deletedFile) {
        return false;
    }

    @Override
    public void close() {
        // NoOp
    }
}