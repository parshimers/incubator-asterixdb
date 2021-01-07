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

import static org.apache.asterix.api.http.server.ServletConstants.SYS_AUTH_HEADER;
import static org.apache.asterix.common.config.ExternalProperties.Option.NC_API_PORT;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.asterix.app.nc.task.BindMetadataNodeTask;
import org.apache.asterix.app.nc.task.CheckpointTask;
import org.apache.asterix.app.nc.task.ExportMetadataNodeTask;
import org.apache.asterix.app.nc.task.LocalRecoveryTask;
import org.apache.asterix.app.nc.task.MetadataBootstrapTask;
import org.apache.asterix.app.nc.task.RetrieveLibrariesTask;
import org.apache.asterix.app.nc.task.StartLifecycleComponentsTask;
import org.apache.asterix.app.nc.task.StartReplicationServiceTask;
import org.apache.asterix.app.nc.task.UpdateNodeStatusTask;
import org.apache.asterix.app.replication.message.MetadataNodeRequestMessage;
import org.apache.asterix.app.replication.message.MetadataNodeResponseMessage;
import org.apache.asterix.app.replication.message.NCLifecycleTaskReportMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksRequestMessage;
import org.apache.asterix.app.replication.message.RegistrationTasksResponseMessage;
import org.apache.asterix.common.api.INCLifecycleTask;
import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.messaging.api.ICCMessageBroker;
import org.apache.asterix.common.replication.INCLifecycleMessage;
import org.apache.asterix.common.replication.INcLifecycleCoordinator;
import org.apache.asterix.common.transactions.IRecoveryManager.SystemState;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.http.HttpHeaders;
import org.apache.http.client.utils.URIBuilder;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.ICCServiceContext;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.control.IGatekeeper;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpScheme;

public class NcLifecycleCoordinator implements INcLifecycleCoordinator {

    private static final Logger LOGGER = LogManager.getLogger();
    protected IClusterStateManager clusterManager;
    protected volatile String metadataNodeId;
    protected Set<String> pendingStartupCompletionNodes = Collections.synchronizedSet(new HashSet<>());
    protected final ICCMessageBroker messageBroker;
    private final boolean replicationEnabled;
    private final IGatekeeper gatekeeper;
    Map<String, Map<String, Object>> nodeSecretsMap;

    public NcLifecycleCoordinator(ICCServiceContext serviceCtx, boolean replicationEnabled) {
        this.messageBroker = (ICCMessageBroker) serviceCtx.getMessageBroker();
        this.replicationEnabled = replicationEnabled;
        this.gatekeeper =
                ((ClusterControllerService) serviceCtx.getControllerService()).getApplication().getGatekeeper();
        this.nodeSecretsMap = new HashMap<>();
    }

    @Override
    public void notifyNodeJoin(String nodeId) {
        pendingStartupCompletionNodes.add(nodeId);
    }

    @Override
    public void notifyNodeFailure(String nodeId) throws HyracksDataException {
        pendingStartupCompletionNodes.remove(nodeId);
        clusterManager.updateNodeState(nodeId, false, null);
        if (nodeId.equals(metadataNodeId)) {
            clusterManager.updateMetadataNode(metadataNodeId, false);
        }
        clusterManager.refreshState();
    }

    @Override
    public void process(INCLifecycleMessage message) throws HyracksDataException {
        switch (message.getType()) {
            case REGISTRATION_TASKS_REQUEST:
                process((RegistrationTasksRequestMessage) message);
                break;
            case REGISTRATION_TASKS_RESULT:
                process((NCLifecycleTaskReportMessage) message);
                break;
            case METADATA_NODE_RESPONSE:
                process((MetadataNodeResponseMessage) message);
                break;
            default:
                throw new RuntimeDataException(ErrorCode.UNSUPPORTED_MESSAGE_TYPE, message.getType().name());
        }
    }

    @Override
    public void bindTo(IClusterStateManager clusterManager) {
        this.clusterManager = clusterManager;
        metadataNodeId = clusterManager.getCurrentMetadataNodeId();
    }

    private void process(RegistrationTasksRequestMessage msg) throws HyracksDataException {
        final String nodeId = msg.getNodeId();
        nodeSecretsMap.put(nodeId, msg.getSecrets());
        List<INCLifecycleTask> tasks = buildNCRegTasks(msg.getNodeId(), msg.getNodeStatus(), msg.getState());
        RegistrationTasksResponseMessage response = new RegistrationTasksResponseMessage(nodeId, tasks);
        try {
            messageBroker.sendApplicationMessageToNC(response, msg.getNodeId());
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    private void process(NCLifecycleTaskReportMessage msg) throws HyracksDataException {
        if (!pendingStartupCompletionNodes.remove(msg.getNodeId())) {
            LOGGER.warn("Received unexpected startup completion message from node {}", msg.getNodeId());
        }
        if (!gatekeeper.isAuthorized(msg.getNodeId())) {
            LOGGER.warn("Node {} lost authorization before startup completed; ignoring registration result",
                    msg.getNodeId());
            return;
        }
        if (msg.isSuccess()) {
            clusterManager.updateNodeState(msg.getNodeId(), true, msg.getLocalCounters());
            if (msg.getNodeId().equals(metadataNodeId)) {
                clusterManager.updateMetadataNode(metadataNodeId, true);
            }
            clusterManager.refreshState();
        } else {
            LOGGER.error("Node {} failed to complete startup", msg.getNodeId(), msg.getException());
        }
    }

    protected List<INCLifecycleTask> buildNCRegTasks(String nodeId, NodeStatus nodeStatus, SystemState state) {
        LOGGER.info("Building registration tasks for node {} with status {} and system state: {}", nodeId, nodeStatus,
                state);
        final boolean isMetadataNode = nodeId.equals(metadataNodeId);
        switch (nodeStatus) {
            case ACTIVE:
                return buildActiveNCRegTasks(isMetadataNode);
            case IDLE:
                return buildIdleNcRegTasks(nodeId, isMetadataNode, state);
            default:
                return new ArrayList<>();
        }
    }

    protected List<INCLifecycleTask> buildActiveNCRegTasks(boolean metadataNode) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        if (metadataNode) {
            tasks.add(new BindMetadataNodeTask());
        }
        return tasks;
    }

    @Override
    public synchronized void notifyMetadataNodeChange(String node) throws HyracksDataException {
        if (metadataNodeId.equals(node)) {
            return;
        }
        // if current metadata node is active, we need to unbind its metadata proxy objects
        if (clusterManager.isMetadataNodeActive()) {
            MetadataNodeRequestMessage msg =
                    new MetadataNodeRequestMessage(false, clusterManager.getMetadataPartition().getPartitionId());
            try {
                messageBroker.sendApplicationMessageToNC(msg, metadataNodeId);
                // when the current node responses, we will bind to the new one
                metadataNodeId = node;
            } catch (Exception e) {
                throw HyracksDataException.create(e);
            }
        } else {
            requestMetadataNodeTakeover(node);
        }
    }

    protected List<INCLifecycleTask> buildIdleNcRegTasks(String newNodeId, boolean metadataNode, SystemState state) {
        final List<INCLifecycleTask> tasks = new ArrayList<>();
        tasks.add(new UpdateNodeStatusTask(NodeStatus.BOOTING));
        if (state == SystemState.CORRUPTED) {
            // need to perform local recovery for node partitions
            LocalRecoveryTask rt = new LocalRecoveryTask(Arrays.stream(clusterManager.getNodePartitions(newNodeId))
                    .map(ClusterPartition::getPartitionId).collect(Collectors.toSet()));
            tasks.add(rt);
        }
        if (replicationEnabled) {
            tasks.add(new StartReplicationServiceTask());
        }
        if (metadataNode) {
            tasks.add(new MetadataBootstrapTask(clusterManager.getMetadataPartition().getPartitionId()));
        }
        tasks.add(new CheckpointTask());
        tasks.add(new StartLifecycleComponentsTask());
        Set<String> nodes = clusterManager.getParticipantNodes(true);
        nodes.remove(newNodeId);
        tasks.add(getLibraryTask(newNodeId, nodes));
        if (metadataNode) {
            tasks.add(new ExportMetadataNodeTask(true));
            tasks.add(new BindMetadataNodeTask());
        }
        tasks.add(new UpdateNodeStatusTask(NodeStatus.ACTIVE));
        return tasks;
    }

    private synchronized void process(MetadataNodeResponseMessage response) throws HyracksDataException {
        // rebind metadata node since it might be changing
        MetadataManager.INSTANCE.rebindMetadataNode();
        clusterManager.updateMetadataNode(response.getNodeId(), response.isExported());
        if (!response.isExported()) {
            requestMetadataNodeTakeover(metadataNodeId);
        }
    }

    protected Map<String, String> getNCAuthToken(String node) {
        return Collections.singletonMap(HttpHeaders.AUTHORIZATION,
                (String) nodeSecretsMap.get(node).get(SYS_AUTH_HEADER));
    }

    private URI constructNCRecoveryUri(String nodeId) {
        Map<IOption, Object> nodeConfig = clusterManager.getNcConfiguration().get(nodeId);
        String host = (String) nodeConfig.get(NCConfig.Option.PUBLIC_ADDRESS);
        int port = (Integer) nodeConfig.get(NC_API_PORT);
        URIBuilder builder = new URIBuilder().setScheme(HttpScheme.HTTP.toString()).setHost(host).setPort(port);
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            LOGGER.error("Could not find URL for NC recovery", e);
        }
        return null;
    }

    private void requestMetadataNodeTakeover(String node) throws HyracksDataException {
        MetadataNodeRequestMessage msg =
                new MetadataNodeRequestMessage(true, clusterManager.getMetadataPartition().getPartitionId());
        try {
            messageBroker.sendApplicationMessageToNC(msg, node);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    protected RetrieveLibrariesTask getLibraryTask(String newNodeId, Set<String> referenceNodes) {
        List<Pair<URI, String>> referenceNodeLocAndAuth = new ArrayList<>();
        for (String node : referenceNodes) {
            referenceNodeLocAndAuth
                    .add(new Pair<>(constructNCRecoveryUri(node), getNCAuthToken(node).get(HttpHeaders.AUTHORIZATION)));
        }
        return new RetrieveLibrariesTask(referenceNodeLocAndAuth, new Pair<>(constructNCRecoveryUri(newNodeId),
                getNCAuthToken(newNodeId).get(HttpHeaders.AUTHORIZATION)));
    }
}
