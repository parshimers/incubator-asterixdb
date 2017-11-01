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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.common.config.MetadataProperties;
import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.metadata.MetadataIndexImmutableProperties;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.NCConfig;

public class MetadataOnlyReplicationStrategy implements IReplicationStrategy {


    private String metadataPrimaryReplicaId;
    private Set<Replica> metadataNodeReplicas;
    MetadataProperties metadataProperties;

    @Override
    public boolean isMatch(int datasetId) {
        return datasetId < MetadataIndexImmutableProperties.FIRST_AVAILABLE_USER_DATASET_ID && datasetId >= 0;
    }

    @Override
    public Set<Replica> getRemoteReplicas(String nodeId) {
        if (nodeId.equals(metadataPrimaryReplicaId)) {
            return metadataNodeReplicas;
        }
        return Collections.emptySet();
    }

    @Override
    public boolean isParticipant(String nodeId) {
        return false;
    }

    @Override
    public Set<Replica> getRemotePrimaryReplicas(String nodeId) {
        if (metadataNodeReplicas.stream().map(Replica::getId).filter(replicaId -> replicaId.equals(nodeId))
                .count() != 0) {
            return metadataNodeReplicas;
        }
        return Collections.emptySet();
    }

    @Override
    public MetadataOnlyReplicationStrategy from(ReplicationProperties p, IConfigManager configManager) throws HyracksDataException {
        metadataProperties = p.getMetadataProperties();
        metadataPrimaryReplicaId = metadataProperties.getMetadataNodeName();

        final Set<Replica> replicas = new HashSet<>();
        for (String nodeId :( ((ConfigManager)(configManager)).getNodeNames())) {
            replicas.add(new Replica(nodeId, ((ConfigManager)configManager).getNodeEffectiveConfig(nodeId).getString(NCConfig.Option.REPLICATION_LISTEN_ADDRESS), ((ConfigManager)configManager).getNodeEffectiveConfig(nodeId).getInt(NCConfig.Option.REPLICATION_LISTEN_PORT)));
        }
        MetadataOnlyReplicationStrategy st = new MetadataOnlyReplicationStrategy();
        st.metadataNodeReplicas = replicas;
        return st;
    }
}
