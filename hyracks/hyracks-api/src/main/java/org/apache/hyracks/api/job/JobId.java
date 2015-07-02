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
package org.apache.hyracks.api.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hyracks.api.io.IWritable;

public final class JobId implements IWritable, Serializable {
    private static final long serialVersionUID = 1L;
    private long id;

    public static JobId create(DataInput dis) throws IOException {
        JobId jobId = new JobId();
        jobId.readFields(dis);
        return jobId;
    }

    private JobId() {

    }

    public JobId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JobId)) {
            return false;
        }
        return ((JobId) o).id == id;
    }

    @Override
    public String toString() {
        return "JID:" + id;
    }

    public static JobId parse(String str) {
        if (str.startsWith("JID:")) {
            str = str.substring(4);
            return new JobId(Long.parseLong(str));
        }
        throw new IllegalArgumentException();
    }

    @Override
    public void writeFields(DataOutput output) throws IOException {
        output.writeLong(id);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        id = input.readLong();
    }
}