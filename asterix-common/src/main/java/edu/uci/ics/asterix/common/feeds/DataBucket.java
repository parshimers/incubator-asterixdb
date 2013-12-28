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
package edu.uci.ics.asterix.common.feeds;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class DataBucket {

    private static AtomicInteger globalBucketId = new AtomicInteger(0);
    private final ByteBuffer buffer;
    private final AtomicInteger readCount;
    private final DataBucketPool pool;
    private int desiredReadCount;
    private ContentType contentType;
    private AtomicInteger bucketId = new AtomicInteger(0);

    public enum ContentType {
        DATA, // data (feed tuple)
        EOD // A signal indicating that there shall be no more data
    }

    public DataBucket(DataBucketPool pool) {
        buffer = ByteBuffer.allocate(32768);
        readCount = new AtomicInteger(0);
        this.pool = pool;
        this.contentType = ContentType.DATA;
        bucketId.set(globalBucketId.incrementAndGet());
    }

    public void reset(ByteBuffer frame) {
        buffer.flip();
        System.arraycopy(frame.array(), 0, buffer.array(), 0, frame.limit());
        buffer.limit(frame.limit());
        buffer.position(0);
    }

    public synchronized void doneReading() {
        if (readCount.incrementAndGet() == desiredReadCount) {
            readCount.set(0);
            pool.returnDataBucket(this);
        }
    }

    public void setDesiredReadCount(int rCount) {
        this.desiredReadCount = rCount;
    }

    public ContentType getContentType() {
        return contentType;
    }

    public void setContentType(ContentType contentType) {
        this.contentType = contentType;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public String toString() {
        return "DataBucket [" + bucketId + "]" + " (" + readCount + "," + desiredReadCount + ")";
    }

}