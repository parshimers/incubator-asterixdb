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
package org.apache.hyracks.control.cc.job;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.graph.EndpointPair;
import com.google.common.graph.Graph;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.client.impl.IOperatorDescriptorVisitor;
import org.apache.hyracks.api.client.impl.JobActivityGraphBuilder;
import org.apache.hyracks.api.client.impl.PlanUtils;
import org.apache.hyracks.api.constraints.Constraint;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.ConnectorDescriptorId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IConnectorDescriptor;
import org.apache.hyracks.api.dataflow.IOperatorDescriptor;
import org.apache.hyracks.api.dataflow.OperatorDescriptorId;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.connectors.IConnectorPolicy;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityCluster;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.ActivityClusterId;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IActivityClusterGraphGenerator;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobActivityGraph;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.profiling.IOperatorStats;
import org.apache.hyracks.api.partitions.PartitionId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.DeployedJobSpecStore.DeployedJobSpecDescriptor;
import org.apache.hyracks.control.cc.executor.ActivityPartitionDetails;
import org.apache.hyracks.control.cc.executor.JobExecutor;
import org.apache.hyracks.control.cc.partitions.PartitionMatchMaker;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.Traverser;

public class JobRun implements IJobStatusConditionVariable {
    private final DeploymentId deploymentId;

    private final JobId jobId;

    private final JobSpecification spec;

    private final ActivityClusterGraph acg;

    private JobExecutor scheduler;

    private final Set<JobFlag> jobFlags;

    private final Map<ActivityClusterId, ActivityClusterPlan> activityClusterPlanMap;

    private final PartitionMatchMaker pmm;

    private final Set<String> participatingNodeIds;

    private final Set<String> cleanupPendingNodeIds;

    private final JobProfile profile;

    private final Map<ConnectorDescriptorId, IConnectorPolicy> connectorPolicyMap;

    private long createTime;

    private long startTime;

    private String startTimeZoneId;

    private long endTime;

    private JobStatus status;

    private List<Exception> exceptions;

    private JobStatus pendingStatus;

    private List<Exception> pendingExceptions;

    private Map<OperatorDescriptorId, Map<Integer, String>> operatorLocations;

    private JobRun(DeploymentId deploymentId, JobId jobId, Set<JobFlag> jobFlags, JobSpecification spec,
            ActivityClusterGraph acg) {
        this.deploymentId = deploymentId;
        this.jobId = jobId;
        this.jobFlags = jobFlags;
        this.spec = spec;
        this.acg = acg;
        activityClusterPlanMap = new HashMap<>();
        pmm = new PartitionMatchMaker();
        participatingNodeIds = new HashSet<>();
        cleanupPendingNodeIds = new HashSet<>();
        connectorPolicyMap = new HashMap<>();
        operatorLocations = new HashMap<>();
        createTime = System.currentTimeMillis();
        profile = new JobProfile(jobId);
        profile.setCreateTime(createTime);
    }

    //Run a deployed job spec
    public JobRun(ClusterControllerService ccs, DeploymentId deploymentId, JobId jobId, Set<JobFlag> jobFlags,
            DeployedJobSpecDescriptor deployedJobSpecDescriptor, Map<byte[], byte[]> jobParameters,
            DeployedJobSpecId deployedJobSpecId) throws HyracksException {
        this(deploymentId, jobId, jobFlags, deployedJobSpecDescriptor.getJobSpecification(),
                deployedJobSpecDescriptor.getActivityClusterGraph());
        ccs.createOrGetJobParameterByteStore(jobId).setParameters(jobParameters);
        Set<Constraint> constaints = deployedJobSpecDescriptor.getActivityClusterGraphConstraints();
        this.scheduler = new JobExecutor(ccs, this, constaints, deployedJobSpecId);
    }

    //Run a new job by creating an ActivityClusterGraph
    public JobRun(ClusterControllerService ccs, DeploymentId deploymentId, JobId jobId,
            IActivityClusterGraphGeneratorFactory acggf, IActivityClusterGraphGenerator acgg, Set<JobFlag> jobFlags) {
        this(deploymentId, jobId, jobFlags, acggf.getJobSpecification(), acgg.initialize());
        this.scheduler = new JobExecutor(ccs, this, acgg.getConstraints(), null);
    }

    public DeploymentId getDeploymentId() {
        return deploymentId;
    }

    public JobSpecification getJobSpecification() {
        return spec;
    }

    public JobId getJobId() {
        return jobId;
    }

    public ActivityClusterGraph getActivityClusterGraph() {
        return acg;
    }

    public Set<JobFlag> getFlags() {
        return jobFlags;
    }

    public Map<ActivityClusterId, ActivityClusterPlan> getActivityClusterPlanMap() {
        return activityClusterPlanMap;
    }

    public PartitionMatchMaker getPartitionMatchMaker() {
        return pmm;
    }

    public synchronized void setStatus(JobStatus status, List<Exception> exceptions) {
        this.status = status;
        this.exceptions = exceptions;
        notifyAll();
    }

    public synchronized JobStatus getStatus() {
        return status;
    }

    public synchronized List<Exception> getExceptions() {
        return exceptions;
    }

    public void setPendingStatus(JobStatus status, List<Exception> exceptions) {
        this.pendingStatus = status;
        this.pendingExceptions = exceptions;
    }

    public JobStatus getPendingStatus() {
        return pendingStatus;
    }

    public synchronized List<Exception> getPendingExceptions() {
        return pendingExceptions;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
        this.profile.setStartTime(startTime);
    }

    public String getStartTimeZoneId() {
        return startTimeZoneId;
    }

    public void setStartTimeZoneId(String startTimeZoneId) {
        this.startTimeZoneId = startTimeZoneId;
        this.profile.setStartTimeZoneId(startTimeZoneId);
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
        this.profile.setEndTime(endTime);
    }

    public void registerOperatorLocation(OperatorDescriptorId op, int partition, String location) {
        operatorLocations.computeIfAbsent(op, k -> new HashMap<>()).put(partition, location);
    }

    @Override
    public synchronized void waitForCompletion() throws Exception {
        while (status == JobStatus.PENDING || status == JobStatus.RUNNING) {
            wait();
        }
        if (exceptions != null && !exceptions.isEmpty()) {
            HyracksException he = HyracksException.create(exceptions.get(0));
            for (int i = 1; i < exceptions.size(); ++i) {
                he.addSuppressed(exceptions.get(i));
            }
            throw he;
        }
    }

    public Set<String> getParticipatingNodeIds() {
        return participatingNodeIds;
    }

    public Set<String> getCleanupPendingNodeIds() {
        return cleanupPendingNodeIds;
    }

    public JobProfile getJobProfile() {
        return profile;
    }

    public JobExecutor getExecutor() {
        return scheduler;
    }

    public Map<ConnectorDescriptorId, IConnectorPolicy> getConnectorPolicyMap() {
        return connectorPolicyMap;
    }

    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode result = om.createObjectNode();

        result.put("job-id", jobId.toString());
        result.putPOJO("status", getStatus());
        result.put("create-time", getCreateTime());
        result.put("start-time", getStartTime());
        result.put("end-time", getEndTime());

        ArrayNode aClusters = om.createArrayNode();
        for (ActivityCluster ac : acg.getActivityClusterMap().values()) {
            ObjectNode acJSON = om.createObjectNode();

            acJSON.put("activity-cluster-id", String.valueOf(ac.getId()));

            ArrayNode activitiesJSON = om.createArrayNode();
            for (ActivityId aid : ac.getActivityMap().keySet()) {
                activitiesJSON.addPOJO(aid);
            }
            acJSON.set("activities", activitiesJSON);

            ArrayNode dependenciesJSON = om.createArrayNode();
            for (ActivityCluster dependency : ac.getDependencies()) {
                dependenciesJSON.add(String.valueOf(dependency.getId()));
            }
            acJSON.set("dependencies", dependenciesJSON);

            ActivityClusterPlan acp = activityClusterPlanMap.get(ac.getId());
            if (acp == null) {
                acJSON.putNull("plan");
            } else {
                ObjectNode planJSON = om.createObjectNode();

                ArrayNode acTasks = om.createArrayNode();
                acp.getActivityPlanMap().forEach((key, acPlan) -> {
                    ObjectNode entry = om.createObjectNode();
                    entry.put("activity-id", key.toString());

                    ActivityPartitionDetails apd = acPlan.getActivityPartitionDetails();
                    entry.put("partition-count", apd.getPartitionCount());

                    ArrayNode inPartCountsJSON = om.createArrayNode();
                    int[] inPartCounts = apd.getInputPartitionCounts();
                    if (inPartCounts != null) {
                        for (int i : inPartCounts) {
                            inPartCountsJSON.add(i);
                        }
                    }
                    entry.set("input-partition-counts", inPartCountsJSON);

                    ArrayNode outPartCountsJSON = om.createArrayNode();
                    int[] outPartCounts = apd.getOutputPartitionCounts();
                    if (outPartCounts != null) {
                        for (int o : outPartCounts) {
                            outPartCountsJSON.add(o);
                        }
                    }
                    entry.set("output-partition-counts", outPartCountsJSON);

                    ArrayNode tasks = om.createArrayNode();
                    for (Task t : acPlan.getTasks()) {
                        ObjectNode task = om.createObjectNode();

                        task.put("task-id", t.getTaskId().toString());

                        ArrayNode dependentTasksJSON = om.createArrayNode();
                        for (TaskId dependent : t.getDependents()) {
                            dependentTasksJSON.add(dependent.toString());
                            task.set("dependents", dependentTasksJSON);

                            ArrayNode dependencyTasksJSON = om.createArrayNode();
                            for (TaskId dependency : t.getDependencies()) {
                                dependencyTasksJSON.add(dependency.toString());
                            }
                            task.set("dependencies", dependencyTasksJSON);

                            tasks.add(task);
                        }
                        entry.set("tasks", tasks);

                        acTasks.add(entry);
                    }
                });
                planJSON.set("activities", acTasks);

                ArrayNode tClusters = om.createArrayNode();
                for (TaskCluster tc : acp.getTaskClusters()) {
                    ObjectNode c = om.createObjectNode();
                    c.put("task-cluster-id", String.valueOf(tc.getTaskClusterId()));

                    ArrayNode tasksAry = om.createArrayNode();
                    for (Task t : tc.getTasks()) {
                        tasksAry.add(t.getTaskId().toString());
                    }
                    c.set("tasks", tasksAry);

                    ArrayNode prodParts = om.createArrayNode();
                    for (PartitionId p : tc.getProducedPartitions()) {
                        prodParts.add(p.toString());
                    }
                    c.set("produced-partitions", prodParts);

                    ArrayNode reqdParts = om.createArrayNode();
                    for (PartitionId p : tc.getRequiredPartitions()) {
                        reqdParts.add(p.toString());
                    }
                    c.set("required-partitions", reqdParts);

                    ArrayNode attempts = om.createArrayNode();
                    List<TaskClusterAttempt> tcAttempts = tc.getAttempts();
                    if (tcAttempts != null) {
                        for (TaskClusterAttempt tca : tcAttempts) {
                            ObjectNode attempt = om.createObjectNode();
                            attempt.put("attempt", tca.getAttempt());
                            attempt.putPOJO("status", tca.getStatus());
                            attempt.put("start-time", tca.getStartTime());
                            attempt.put("end-time", tca.getEndTime());

                            ArrayNode taskAttempts = om.createArrayNode();
                            for (TaskAttempt ta : tca.getTaskAttempts().values()) {
                                ObjectNode taskAttempt = om.createObjectNode();
                                taskAttempt.putPOJO("task-id", ta.getTaskAttemptId().getTaskId());
                                taskAttempt.putPOJO("task-attempt-id", ta.getTaskAttemptId());
                                taskAttempt.putPOJO("status", ta.getStatus());
                                taskAttempt.put("node-id", ta.getNodeId());
                                taskAttempt.put("start-time", ta.getStartTime());
                                taskAttempt.put("end-time", ta.getEndTime());
                                List<Exception> exceptions = ta.getExceptions();
                                if (exceptions != null && !exceptions.isEmpty()) {
                                    List<Exception> filteredExceptions = ExceptionUtils.getActualExceptions(exceptions);
                                    for (Exception exception : filteredExceptions) {
                                        StringWriter exceptionWriter = new StringWriter();
                                        exception.printStackTrace(new PrintWriter(exceptionWriter));
                                        taskAttempt.put("failure-details", exceptionWriter.toString());
                                    }
                                }
                                taskAttempts.add(taskAttempt);
                            }
                            attempt.set("task-attempts", taskAttempts);

                            attempts.add(attempt);
                        }
                    }
                    c.set("attempts", attempts);

                    tClusters.add(c);
                }
                planJSON.set("task-clusters", tClusters);

                acJSON.set("plan", planJSON);
            }
            aClusters.add(acJSON);
        }
        result.set("activity-clusters", aClusters);
        result.set("profile", profile.toJSON());

        return result;
    }

    private JobActivityGraph getJAG() throws HyracksException {
        final JobActivityGraphBuilder builder = new JobActivityGraphBuilder(spec, jobFlags);
        PlanUtils.visit(spec, builder::addConnector);
        PlanUtils.visit(spec, new IOperatorDescriptorVisitor() {
            @Override
            public void visit(IOperatorDescriptor op) {
                op.contributeActivities(builder);
            }
        });
        builder.finish();
        return builder.getActivityGraph();
    }

    public void postProcessing() throws HyracksException {
        //        MutableGraph<OperatorDescriptorId> jobSpec = GraphBuilder.<OperatorDescriptorId> directed().build();
        //        JobSpecification spec = getJobSpecification();
        //        spec.getOperatorMap().keySet().forEach(op -> jobSpec.addNode(op));
        //        spec.getConnectorOperatorMap().values().forEach(p -> {
        //            jobSpec.putEdge(p.getLeft().getKey().getOperatorId(), p.getRight().getKey().getOperatorId());
        //        });
        //        Traverser<OperatorDescriptorId> trav = Traverser.forGraph(jobSpec);
        //        trav.breadthFirst(new OperatorDescriptorId(0));
        JobActivityGraph jobActivityGraph = getJAG();

        MutableGraph<ActivityId> ac = GraphBuilder.<OperatorDescriptorId> directed().build();

        Map<OperatorDescriptorId, IOperatorStats> odidStats = new HashMap<>();
        profile.getJobletProfiles().values().forEach(jp -> jp.getTaskProfiles().values().forEach(tp -> tp
                .getStatsCollector().getAllOperatorStats().values().forEach(st -> odidStats.put(st.getId(), st))));

        List<IConnectorDescriptor> connectors;
        ActivityId fromActivityId;
        ActivityId toActivityId;
        final Set<Pair<ActivityId, ActivityId>> activitiesPairedSet = new HashSet<>();
        final Map<ActivityId, IActivity> activityMap = jobActivityGraph.getActivityMap();
        final Map<ActivityId, List<IConnectorDescriptor>> activityInputMap = jobActivityGraph.getActivityInputMap();
        final Map<ActivityId, List<IConnectorDescriptor>> activityOutputMap = jobActivityGraph.getActivityOutputMap();
        Set<ActivityId> starts = new HashSet<>();

        // go through each activity. First, map its input -> activity, then activity -> its output
        for (Map.Entry<ActivityId, IActivity> entry : activityMap.entrySet()) {
            toActivityId = entry.getValue().getActivityId();
            ac.addNode(toActivityId);
            // process input -> to activity
            connectors = activityInputMap.get(entry.getKey());
            if (connectors != null) {
                for (IConnectorDescriptor connector : connectors) {
                    fromActivityId = jobActivityGraph.getProducerActivity(connector.getConnectorId());
                    Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                    if (!activitiesPairedSet.contains(newPair)) {
                        activitiesPairedSet.add(newPair);
                        ac.putEdge(toActivityId, fromActivityId);
                    }
                }
            }

            // process from activity -> output
            fromActivityId = toActivityId;
            connectors = activityOutputMap.get(entry.getKey());
            if (connectors != null) {
                for (IConnectorDescriptor connector : connectors) {
                    toActivityId = jobActivityGraph.getConsumerActivity(connector.getConnectorId());
                    Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                    if (!activitiesPairedSet.contains(newPair)) {
                        activitiesPairedSet.add(newPair);
                        ac.putEdge(toActivityId, fromActivityId);
                    }
                }
            }
        }

        final Map<ActivityId, Set<ActivityId>> blocked2BlockerMap = jobActivityGraph.getBlocked2BlockerMap();
        for (Map.Entry<ActivityId, Set<ActivityId>> entry : blocked2BlockerMap.entrySet()) {
            toActivityId = entry.getKey();
            for (ActivityId blockingActivityId : entry.getValue()) {
                fromActivityId = blockingActivityId;
                Pair<ActivityId, ActivityId> newPair = new ImmutablePair<>(fromActivityId, toActivityId);
                if (!activitiesPairedSet.contains(newPair)) {
                    activitiesPairedSet.add(newPair);
                    ac.removeEdge(toActivityId, fromActivityId);
                    starts.add(fromActivityId);
                }
            }
        }
        for (ActivityId node : activityMap.keySet()) {
            if (ac.inDegree(node) == 0) {
                starts.add(node);
            }
        }
        for (ActivityId root : starts) {
            for (Iterator<ActivityId> nodes = Traverser.forGraph(ac).breadthFirst(root).iterator(); nodes.hasNext();) {
                ActivityId node = nodes.next();
                IOperatorStats nodeStats = odidStats.get(node.getOperatorDescriptorId());
                long prevTimes = 0;
                for (ActivityId prevNode : ac.successors(node)) {
                    prevTimes += odidStats.get(prevNode.getOperatorDescriptorId()).getTimeCounter().get();
                }
                long totalTime = nodeStats.getTimeCounter().get();
                long delta = prevTimes - totalTime;
                nodeStats.getTimeCounter().set(delta);
            }
        }
    }

    public Map<OperatorDescriptorId, Map<Integer, String>> getOperatorLocations() {
        return operatorLocations;
    }

    public static class DOTWriter {

        public static String write(final Graph<ActivityId> graph) {
            StringBuilder sb = new StringBuilder();
            sb.append("strict digraph G {\n");

            for(ActivityId n : graph.nodes()) {
                sb.append("  \"" + n.toString() + "\" " + fancyLabel(n) + "\n");
            }

            for(EndpointPair<ActivityId> e : graph.edges()) {
                sb.append("  \"" + e.source().toString() + "\" -> \"" + e.target().toString() + "\"\n");
            }

            sb.append("}");
            return sb.toString();
        }

        private static String fancyLabel(final ActivityId node) {
            return String.format("[ label=\"%s\" ]",
                    node.toString());
        }
    }
}
