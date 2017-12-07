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
package org.apache.asterix.app.replication;

import org.apache.asterix.common.config.ReplicationProperties;
import org.apache.asterix.common.replication.IFaultToleranceStrategy;
import org.apache.asterix.common.replication.IReplicationStrategy;
import org.apache.hyracks.api.application.ICCServiceContext;

public class FaultToleranceStrategyFactory {

    private FaultToleranceStrategyFactory() {
        throw new AssertionError();
    }

    public static IFaultToleranceStrategy create(ICCServiceContext serviceCtx, ReplicationProperties repProp,
            IReplicationStrategy strategy) {
        Class<? extends IFaultToleranceStrategy> clazz;
        if (!repProp.isReplicationEnabled()) {
            clazz = NoFaultToleranceStrategy.class;
        } else {
            if ("metadata_only".equals(repProp.getReplicationStrategy())) {
                clazz = MetadataNodeFaultToleranceStrategy.class;
            } else {
                clazz = AutoFaultToleranceStrategy.class;
            }
        }
        try {
            return clazz.newInstance().from(serviceCtx, strategy);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}