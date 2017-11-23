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

import java.util.Collections;
import java.util.Set;

import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.NCConfig;

public class NoReplicationStrategy implements IReplicationStrategy {

    @Override
    public boolean isMatch(int datasetId) {
        return false;
    }

    @Override
    public boolean isParticipant(String nodeId) {
        return false;
    }

    @Override
    public Set<Replica> getRemotePrimaryReplicas(String nodeId) {
        return Collections.emptySet();
    }

    @Override
    public Set<Replica> getRemoteReplicas(String node) {
        return Collections.emptySet();
    }

    public Set<Replica> getRemoteReplicasAndSelf(String nodeId) {
        return Collections.emptySet();
    }

    @Override
    public NoReplicationStrategy from(ReplicationProperties p, IConfigManager configManager) {
        return new NoReplicationStrategy();
    }
}