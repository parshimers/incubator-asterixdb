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
package org.apache.asterix.cloud.clients.profiler;

import org.apache.asterix.cloud.clients.profiler.limiter.IRequestRateLimiter;

public final class RequestLimiterNoOpProfiler implements IRequestProfilerLimiter {
    private final IRequestRateLimiter limiter;

    public RequestLimiterNoOpProfiler(IRequestRateLimiter limiter) {
        this.limiter = limiter;
    }

    @Override
    public void objectsList() {
        limiter.listRequest();
    }

    @Override
    public void objectGet() {
        limiter.readRequest();
    }

    @Override
    public void objectWrite() {
        limiter.writeRequest();
    }

    @Override
    public void objectDelete() {
        limiter.writeRequest();
    }

    @Override
    public void objectCopy() {
        limiter.writeRequest();
    }

    @Override
    public void objectMultipartUpload() {
        limiter.writeRequest();
    }

    @Override
    public void objectMultipartDownload() {
        limiter.writeRequest();
    }

    @Override
    public long objectsListCount() {
        return 0;
    }

    @Override
    public long objectGetCount() {
        return 0;
    }

    @Override
    public long objectWriteCount() {
        return 0;
    }

    @Override
    public long objectDeleteCount() {
        return 0;
    }

    @Override
    public long objectCopyCount() {
        return 0;
    }

    @Override
    public long objectMultipartUploadCount() {
        return 0;
    }

    @Override
    public long objectMultipartDownloadCount() {
        return 0;
    }
}