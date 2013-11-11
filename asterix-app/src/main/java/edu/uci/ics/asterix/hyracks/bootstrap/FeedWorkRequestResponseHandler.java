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
package edu.uci.ics.asterix.hyracks.bootstrap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedCollectInfo;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedFailure;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedIntakeInfo;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedJobNotificationHandler;
import edu.uci.ics.asterix.hyracks.bootstrap.FeedLifecycleListener.FeedsDeActivator;
import edu.uci.ics.asterix.metadata.api.IClusterManagementWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWork;
import edu.uci.ics.asterix.metadata.cluster.AddNodeWorkResponse;
import edu.uci.ics.asterix.metadata.cluster.IClusterManagementWorkResponse;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.asterix.om.util.AsterixClusterProperties;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.constraints.Constraint;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstantExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.ConstraintExpression.ExpressionTag;
import edu.uci.ics.hyracks.api.constraints.expressions.LValueConstraintExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionCountExpression;
import edu.uci.ics.hyracks.api.constraints.expressions.PartitionLocationExpression;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedWorkRequestResponseHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(FeedWorkRequestResponseHandler.class.getName());

    private final LinkedBlockingQueue<IClusterManagementWorkResponse> inbox;

    private Map<Integer, Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>>> feedsWaitingForResponse = new HashMap<Integer, Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>>>();

    public FeedWorkRequestResponseHandler(LinkedBlockingQueue<IClusterManagementWorkResponse> inbox,
            FeedJobNotificationHandler feedJobNotificationHandler) {
        this.inbox = inbox;
    }

    @Override
    public void run() {
        while (true) {
            IClusterManagementWorkResponse response = null;
            try {
                response = inbox.take();
            } catch (InterruptedException e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Interrupted exception " + e.getMessage());
                }
            }
            IClusterManagementWork submittedWork = response.getWork();
            Map<String, String> nodeSubstitution = new HashMap<String, String>();
            switch (submittedWork.getClusterManagementWorkType()) {
                case ADD_NODE:
                    AddNodeWork addNodeWork = (AddNodeWork) submittedWork;
                    int workId = addNodeWork.getWorkId();
                    Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> failureReport = feedsWaitingForResponse
                            .get(workId);
                    AddNodeWorkResponse resp = (AddNodeWorkResponse) response;
                    List<String> nodesAdded = resp.getNodesAdded();
                    int nodeIndex = 0;
                    List<String> unsubstitutedNodes = new ArrayList<String>();
                    unsubstitutedNodes.addAll(failureReport.keySet());
                    if (nodesAdded != null && nodesAdded.size() > 0) {
                        for (String failedNodeId : failureReport.keySet()) {
                            String substitute = nodesAdded.get(nodeIndex);
                            nodeSubstitution.put(failedNodeId, substitute);
                            nodeIndex = (nodeIndex + 1) % nodesAdded.size();
                            unsubstitutedNodes.remove(failedNodeId);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Node " + substitute + " chosen to substiute lost node " + failedNodeId);
                            }
                        }
                    }
                    if (unsubstitutedNodes.size() > 0) {
                        String[] participantNodes = AsterixClusterProperties.INSTANCE.getParticipantNodes().toArray(
                                new String[] {});
                        nodeIndex = 0;
                        for (String unsubstitutedNode : unsubstitutedNodes) {
                            nodeSubstitution.put(unsubstitutedNode, participantNodes[nodeIndex]);
                            nodeIndex = (nodeIndex + 1) % participantNodes.length;
                        }
                    }

                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.warning("Request " + resp.getWork() + " completed using internal nodes");
                    }

                    List<FeedIntakeInfo> feedIntakeJobInfos = new ArrayList<FeedIntakeInfo>();
                    List<FeedCollectInfo> feedCollectJobInfos = new ArrayList<FeedCollectInfo>();

                    // alter failed feed intake jobs
                    for (Entry<String, String> entry : nodeSubstitution.entrySet()) {
                        String failedNode = entry.getKey();
                        Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>> p = failureReport.get(failedNode);
                        List<FeedIntakeInfo> feedIntakeInfos = p.first;
                        for (FeedIntakeInfo feedIntakeInfo : feedIntakeInfos) {
                            replaceNode(feedIntakeInfo.jobSpec, failedNode, entry.getValue());
                            feedIntakeJobInfos.add(feedIntakeInfo);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Replaced node " + failedNode + " with " + entry.getValue()
                                        + " for feed intake [" + feedIntakeInfo.feedId + "]");
                            }
                        }

                        List<FeedCollectInfo> feedCollectInfos = p.second;
                        for (FeedCollectInfo feedCollectInfo : feedCollectInfos) {
                            replaceNode(feedCollectInfo.jobSpec, failedNode, entry.getValue());
                            feedCollectJobInfos.add(feedCollectInfo);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Replaced node " + failedNode + " with " + entry.getValue()
                                        + " for feed collect [" + feedCollectInfo.feedConnectionId + "]");
                            }
                        }
                    }

                    // launch feed intake jobs followed by feed collect jobs
                    for (FeedIntakeInfo feedInfo : feedIntakeJobInfos) {
                        try {
                            AsterixAppContextInfo.getInstance().getHcc().startJob(feedInfo.jobSpec);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Launched feed intake jobs [" + feedInfo + "] post failure");
                            }
                        } catch (Exception e) {
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Exception in launching feed intake job " + feedInfo.feedId);
                            }
                        }
                    }

                    for (FeedCollectInfo feedCollectInfo : feedCollectJobInfos) {
                        try {
                            AsterixAppContextInfo.getInstance().getHcc().startJob(feedCollectInfo.jobSpec);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Launched feed collect info [" + feedCollectInfo + "] jobs post failure");
                            }
                        } catch (Exception e) {
                            if (LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.warning("Exception in launching feed collect job "
                                        + feedCollectInfo.feedConnectionId);
                            }
                        }
                    }
                    break;
                case REMOVE_NODE:
                    throw new IllegalStateException("Invalid work submitted");
            }
        }
    }

    private void replaceNode(JobSpecification jobSpec, String failedNodeId, String replacementNode) {
        Set<Constraint> userConstraints = jobSpec.getUserConstraints();
        List<Constraint> locationConstraintsToReplace = new ArrayList<Constraint>();
        List<Constraint> countConstraintsToReplace = new ArrayList<Constraint>();
        List<OperatorDescriptorId> modifiedOperators = new ArrayList<OperatorDescriptorId>();
        Map<OperatorDescriptorId, List<Constraint>> candidateConstraints = new HashMap<OperatorDescriptorId, List<Constraint>>();
        Map<OperatorDescriptorId, Map<Integer, String>> newConstraints = new HashMap<OperatorDescriptorId, Map<Integer, String>>();
        OperatorDescriptorId opId = null;
        for (Constraint constraint : userConstraints) {
            LValueConstraintExpression lexpr = constraint.getLValue();
            ConstraintExpression cexpr = constraint.getRValue();
            switch (lexpr.getTag()) {
                case PARTITION_COUNT:
                    opId = ((PartitionCountExpression) lexpr).getOperatorDescriptorId();
                    if (modifiedOperators.contains(opId)) {
                        countConstraintsToReplace.add(constraint);
                    } else {
                        List<Constraint> clist = candidateConstraints.get(opId);
                        if (clist == null) {
                            clist = new ArrayList<Constraint>();
                            candidateConstraints.put(opId, clist);
                        }
                        clist.add(constraint);
                    }
                    break;
                case PARTITION_LOCATION:
                    opId = ((PartitionLocationExpression) lexpr).getOperatorDescriptorId();
                    String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                    if (oldLocation.equals(failedNodeId)) {
                        locationConstraintsToReplace.add(constraint);
                        modifiedOperators.add(((PartitionLocationExpression) lexpr).getOperatorDescriptorId());
                        Map<Integer, String> newLocs = newConstraints.get(opId);
                        if (newLocs == null) {
                            newLocs = new HashMap<Integer, String>();
                            newConstraints.put(opId, newLocs);
                        }
                        int partition = ((PartitionLocationExpression) lexpr).getPartition();
                        newLocs.put(partition, replacementNode);
                    } else {
                        if (modifiedOperators.contains(opId)) {
                            locationConstraintsToReplace.add(constraint);
                            Map<Integer, String> newLocs = newConstraints.get(opId);
                            if (newLocs == null) {
                                newLocs = new HashMap<Integer, String>();
                                newConstraints.put(opId, newLocs);
                            }
                            int partition = ((PartitionLocationExpression) lexpr).getPartition();
                            newLocs.put(partition, oldLocation);
                        } else {
                            List<Constraint> clist = candidateConstraints.get(opId);
                            if (clist == null) {
                                clist = new ArrayList<Constraint>();
                                candidateConstraints.put(opId, clist);
                            }
                            clist.add(constraint);
                        }
                    }
                    break;
            }
        }

        jobSpec.getUserConstraints().removeAll(locationConstraintsToReplace);
        jobSpec.getUserConstraints().removeAll(countConstraintsToReplace);

        for (OperatorDescriptorId mopId : modifiedOperators) {
            List<Constraint> clist = candidateConstraints.get(mopId);
            if (clist != null && !clist.isEmpty()) {
                jobSpec.getUserConstraints().removeAll(clist);

                for (Constraint c : clist) {
                    if (c.getLValue().getTag().equals(ExpressionTag.PARTITION_LOCATION)) {
                        ConstraintExpression cexpr = c.getRValue();
                        int partition = ((PartitionLocationExpression) c.getLValue()).getPartition();
                        String oldLocation = (String) ((ConstantExpression) cexpr).getValue();
                        newConstraints.get(mopId).put(partition, oldLocation);
                    }
                }
            }
        }

        for (Entry<OperatorDescriptorId, Map<Integer, String>> entry : newConstraints.entrySet()) {
            OperatorDescriptorId nopId = entry.getKey();
            Map<Integer, String> clist = entry.getValue();
            IOperatorDescriptor op = jobSpec.getOperatorMap().get(nopId);
            String[] locations = new String[clist.size()];
            for (int i = 0; i < locations.length; i++) {
                locations[i] = clist.get(i);
            }
            PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, op, locations);
        }

    }

    public void registerFeedWork(int workId,
            Map<String, Pair<List<FeedIntakeInfo>, List<FeedCollectInfo>>> failureReport) {
        feedsWaitingForResponse.put(workId, failureReport);
    }
}
