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
package org.apache.asterix.common.transactions;

import java.io.Serializable;

public class JobThreadId implements Serializable {
    private static final long serialVersionUID = 1L;

    private JobId jobId;
    private long threadId;
    
    public JobThreadId() {
        this.jobId = new JobId(-1);
        this.threadId = -1;
    }

    public JobThreadId(int jobId) {
        this.jobId = new JobId(jobId);
        this.threadId = Thread.currentThread().getId();
    }

    public JobId getJobId() {
        return jobId;
    }
    
    public long getThreadId() {
        return threadId;
    }
    
    public void setJobId(int jobId) {
        this.jobId.setId(jobId);
    }
    
    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }
    
    public void reset() {
        this.jobId.setId(-1);
        this.threadId = -1;
    }
    
    @Override
    public int hashCode() {
        return jobId.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof JobThreadId)) {
            return false;
        }
        return ((JobThreadId) o).jobId.equals(jobId) && ((JobThreadId) o).threadId == threadId;
    }

    @Override
    public String toString() {
        return "" + jobId + ", ThreadID:" + threadId;
    }
}