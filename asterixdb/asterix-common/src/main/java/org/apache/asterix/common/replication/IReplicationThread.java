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
package org.apache.asterix.common.replication;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.asterix.common.transactions.LogRecord;

public interface IReplicationThread extends Runnable {

    /**
     * Sends a notification to this thread that logRecord has been flushed.
     *
     * @param logRecord The log that has been flushed.
     */
    void notifyLogReplicationRequester(LogRecord logRecord);

    /**
     * @return The replication socket channel.
     */
    SocketChannel getChannel();

    /**
     * Gets a reusable buffer that can be used to send data
     *
     * @return the reusable buffer
     */
    ByteBuffer getReusableBuffer();
}
