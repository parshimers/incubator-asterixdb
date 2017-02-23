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
package org.apache.asterix.common.messaging;

import org.apache.asterix.common.config.AsterixStorageProperties;
import org.apache.asterix.common.config.AsterixTransactionProperties;

import java.util.List;

public class TakeoverPartitionsRequestMessage extends AbstractApplicationMessage {

    private static final long serialVersionUID = 1L;
    private final Integer[] partitions;
    private final long requestId;
    private final String nodeId;
    private final AsterixStorageProperties storageProps;
    private final List<String> nodeLogsToTakeover;

    public TakeoverPartitionsRequestMessage(long requestId, String nodeId, Integer[] partitionsToTakeover,
            List<String> nodeLogsToTakeover, AsterixStorageProperties storageProps) {
        this.requestId = requestId;
        this.nodeId = nodeId;
        this.partitions = partitionsToTakeover;
        this.nodeLogsToTakeover = nodeLogsToTakeover;
        this.storageProps = storageProps;

    }

    @Override
    public ApplicationMessageType getMessageType() {
        return ApplicationMessageType.TAKEOVER_PARTITIONS_REQUEST;
    }

    public Integer[] getPartitions() {
        return partitions;
    }

    public long getRequestId() {
        return requestId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public AsterixStorageProperties getStorageProps() {
        return storageProps;
    }

    public List<String> getNodeLogsToTakeover() {
        return nodeLogsToTakeover;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Request ID: " + requestId);
        sb.append(" Node ID: " + nodeId);
        sb.append(" Partitions: ");
        for (Integer partitionId : partitions) {
            sb.append(partitionId + ",");
        }
        //remove last comma
        sb.charAt(sb.length() - 1);
        return sb.toString();
    }
}
