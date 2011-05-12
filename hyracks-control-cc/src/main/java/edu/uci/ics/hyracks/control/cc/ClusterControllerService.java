/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc;

import java.io.File;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.ClusterControllerInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IJobStatusConditionVariable;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ApplicationDestroyEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ApplicationStartEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobStatusConditionVariableEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobStatusEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCreateEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobStartEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.NodeHeartbeatEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RegisterNodeEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RegisterPartitionAvailibilityEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RegisterPartitionRequestEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RemoveDeadNodesEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ReportProfilesEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.TaskCompleteEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.TaskFailureEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.UnregisterNodeEvent;
import edu.uci.ics.hyracks.control.cc.jobqueue.FutureValue;
import edu.uci.ics.hyracks.control.cc.jobqueue.JobQueue;
import edu.uci.ics.hyracks.control.cc.scheduler.DefaultJobScheduler;
import edu.uci.ics.hyracks.control.cc.scheduler.IJobScheduler;
import edu.uci.ics.hyracks.control.cc.web.WebServer;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController,
        IHyracksClientInterface {
    private static final long serialVersionUID = 1L;

    private CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final Map<String, NodeControllerState> nodeRegistry;

    private final Map<String, Set<String>> ipAddressNodeNameMap;

    private final Map<String, CCApplicationContext> applications;

    private final ServerContext serverCtx;

    private final WebServer webServer;

    private ClusterControllerInfo info;

    private final Map<UUID, JobRun> runMap;

    private final JobQueue jobQueue;

    private final IJobScheduler scheduler;

    private final Executor taskExecutor;

    private final Timer timer;

    private final CCClientInterface ccci;

    private final ICCContext ccContext;

    public ClusterControllerService(CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        nodeRegistry = new LinkedHashMap<String, NodeControllerState>();
        ipAddressNodeNameMap = new HashMap<String, Set<String>>();
        applications = new Hashtable<String, CCApplicationContext>();
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(
                ClusterControllerService.class.getName()));
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer(this);
        runMap = new HashMap<UUID, JobRun>();
        jobQueue = new JobQueue();
        scheduler = new DefaultJobScheduler(this);
        this.timer = new Timer(true);
        ccci = new CCClientInterface(this);
        ccContext = new ICCContext() {
            @Override
            public Map<String, Set<String>> getIPAddressNodeMap() {
                return ipAddressNodeNameMap;
            }
        };
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService");
        Registry registry = LocateRegistry.createRegistry(ccConfig.port);
        registry.rebind(IHyracksClientInterface.class.getName(), ccci);
        registry.rebind(IClusterController.class.getName(), this);
        webServer.setPort(ccConfig.httpPort);
        webServer.start();
        info = new ClusterControllerInfo();
        info.setWebPort(webServer.getListeningPort());
        timer.schedule(new DeadNodeSweeper(), 0, ccConfig.heartbeatPeriod);
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping ClusterControllerService");
        webServer.stop();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    public Map<String, CCApplicationContext> getApplicationMap() {
        return applications;
    }

    public Map<UUID, JobRun> getRunMap() {
        return runMap;
    }

    public JobQueue getJobQueue() {
        return jobQueue;
    }

    public IJobScheduler getScheduler() {
        return scheduler;
    }

    public Executor getExecutor() {
        return taskExecutor;
    }

    public Map<String, NodeControllerState> getNodeMap() {
        return nodeRegistry;
    }

    public Map<String, Set<String>> getIPAddressNodeNameMap() {
        return ipAddressNodeNameMap;
    }

    public CCConfig getConfig() {
        return ccConfig;
    }

    @Override
    public UUID createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        UUID jobId = UUID.randomUUID();
        JobCreateEvent jce = new JobCreateEvent(this, jobId, appName, jobSpec, jobFlags);
        jobQueue.schedule(jce);
        jce.sync();
        return jobId;
    }

    @Override
    public NodeParameters registerNode(NodeRegistration reg) throws Exception {
        INodeController nodeController = reg.getNodeController();
        String id = reg.getNodeId();
        NCConfig ncConfig = reg.getNCConfig();
        NetworkAddress dataPort = reg.getDataPort();
        NodeControllerState state = new NodeControllerState(nodeController, ncConfig, dataPort);
        jobQueue.scheduleAndSync(new RegisterNodeEvent(this, id, state));
        nodeController.notifyRegistration(this);
        LOGGER.log(Level.INFO, "Registered INodeController: id = " + id);
        NodeParameters params = new NodeParameters();
        params.setClusterControllerInfo(info);
        params.setHeartbeatPeriod(ccConfig.heartbeatPeriod);
        params.setProfileDumpPeriod(ccConfig.profileDumpPeriod);
        return params;
    }

    @Override
    public void unregisterNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        jobQueue.scheduleAndSync(new UnregisterNodeEvent(this, id));
        LOGGER.log(Level.INFO, "Unregistered INodeController");
    }

    @Override
    public void notifyTaskComplete(UUID jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        TaskCompleteEvent sce = new TaskCompleteEvent(this, jobId, taskId, nodeId);
        jobQueue.schedule(sce);
    }

    @Override
    public void notifyTaskFailure(UUID jobId, TaskAttemptId taskId, String nodeId, Exception exception)
            throws Exception {
        TaskFailureEvent tfe = new TaskFailureEvent(this, jobId, taskId, nodeId, exception);
        jobQueue.schedule(tfe);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        GetJobStatusEvent gse = new GetJobStatusEvent(this, jobId);
        jobQueue.scheduleAndSync(gse);
        return gse.getStatus();
    }

    @Override
    public void start(UUID jobId) throws Exception {
        JobStartEvent jse = new JobStartEvent(this, jobId);
        jobQueue.schedule(jse);
    }

    @Override
    public void waitForCompletion(UUID jobId) throws Exception {
        GetJobStatusConditionVariableEvent e = new GetJobStatusConditionVariableEvent(this, jobId);
        jobQueue.scheduleAndSync(e);
        IJobStatusConditionVariable var = e.getConditionVariable();
        if (var != null) {
            var.waitForCompletion();
        }
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        jobQueue.schedule(new ReportProfilesEvent(this, profiles));
    }

    @Override
    public synchronized void nodeHeartbeat(String id) throws Exception {
        jobQueue.schedule(new NodeHeartbeatEvent(this, id));
    }

    @Override
    public void createApplication(String appName) throws Exception {
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            CCApplicationContext appCtx = new CCApplicationContext(serverCtx, ccContext, appName);
            applications.put(appName, appCtx);
        }
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        FutureValue fv = new FutureValue();
        jobQueue.schedule(new ApplicationDestroyEvent(this, appName, fv));
        fv.get();
    }

    @Override
    public void startApplication(final String appName) throws Exception {
        FutureValue fv = new FutureValue();
        jobQueue.schedule(new ApplicationStartEvent(this, appName, fv));
        fv.get();
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        return info;
    }

    @Override
    public void registerPartitionProvider(PartitionId pid, String nodeId) throws Exception {
        jobQueue.schedule(new RegisterPartitionAvailibilityEvent(this, pid, nodeId));
    }

    @Override
    public void registerPartitionRequest(Collection<PartitionId> requiredPartitionIds, String nodeId) {
        jobQueue.schedule(new RegisterPartitionRequestEvent(this, requiredPartitionIds, nodeId));
    }

    private class DeadNodeSweeper extends TimerTask {
        @Override
        public void run() {
            jobQueue.schedule(new RemoveDeadNodesEvent(ClusterControllerService.this));
        }
    }
}