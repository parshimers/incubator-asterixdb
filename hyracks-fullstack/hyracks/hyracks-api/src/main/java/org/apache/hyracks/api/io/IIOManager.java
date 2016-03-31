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
package org.apache.hyracks.api.io;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IIOManager {
    public enum FileReadWriteMode {
        READ_ONLY,
        READ_WRITE
    }

    public enum FileSyncMode {
        METADATA_SYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_SYNC,
        METADATA_ASYNC_DATA_ASYNC
    }

    public List<IODeviceHandle> getIODevices();

    public IFileHandle open(FileReference fileRef, FileReadWriteMode rwMode, FileSyncMode syncMode)
            throws HyracksDataException;

    public int syncWrite(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    public int syncRead(IFileHandle fHandle, long offset, ByteBuffer data) throws HyracksDataException;

    public int append(IFileHandle fhandle, ByteBuffer data) throws HyracksDataException;

    public IIOFuture asyncWrite(IFileHandle fHandle, long offset, ByteBuffer data);

    public IIOFuture asyncRead(IFileHandle fHandle, long offset, ByteBuffer data);

    public void close(IFileHandle fHandle) throws HyracksDataException;

    public void sync(IFileHandle fileHandle, boolean metadata) throws HyracksDataException;

    public void setExecutor(Executor executor);

    public long getSize(IFileHandle fileHandle) throws HyracksDataException;

    public boolean delete(FileReference fileReference);

    public boolean delete(FileReference fileReference, boolean recursive);

    public boolean exists(FileReference fileReference);

    public boolean mkdirs(FileReference fileReference);

    public boolean isDirectory(FileReference fileReference);

    boolean deleteOnExit(FileReference fileReference);

    FileReference getParent(FileReference child);

    public String[] listFiles(FileReference fileReference, FilenameFilter transactionFileNameFilter) throws HyracksDataException;

    public InputStream getInputStream(IFileHandle fileHandle);

    public void deleteWorkspaceFiles();

    /**
     * @param ioDeviceId
     * @param relativePath
     * @return A file reference based on the mounting point of {@code ioDeviceId} and the passed {@code relativePath}
     */
    public FileReference getAbsoluteFileRef(int ioDeviceId, String relativePath);
}
