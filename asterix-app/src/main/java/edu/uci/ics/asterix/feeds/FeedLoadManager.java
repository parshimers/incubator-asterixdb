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
package edu.uci.ics.asterix.feeds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedCongestionMessage;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.file.FeedOperations;
import edu.uci.ics.asterix.metadata.feeds.FeedUtil;
import edu.uci.ics.asterix.metadata.feeds.PrepareStallMessage;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedLoadManager implements IFeedLoadManager {

    private static final Logger LOGGER = Logger.getLogger(FeedLoadManager.class.getName());

    public enum FeedJobState {
        ACTIVE,
        CONGESTION_REPORTED,
        UNDER_RECOVERY
    }

    private final TreeSet<NodeLoadReport> nodeReports;

    private final Map<FeedConnectionId, FeedJobState> jobStates;

    public FeedLoadManager() {
        nodeReports = new TreeSet<NodeLoadReport>();
        jobStates = new HashMap<FeedConnectionId, FeedJobState>();
    }

    @Override
    public void submitNodeLoadReport(NodeLoadReport report) {
        nodeReports.remove(report);
        nodeReports.add(report);
    }

    @Override
    public void reportCongestion(FeedCongestionMessage message) throws AsterixException {
        FeedRuntimeId runtimeId = message.getRuntimeId();
        FeedJobState jobState = jobStates.get(runtimeId.getConnectionId());
        if (jobState != null
                && (jobState.equals(FeedJobState.CONGESTION_REPORTED) || jobState.equals(FeedJobState.UNDER_RECOVERY))) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Ignoring congestion report from " + runtimeId + " as job is already under recovery");
            }
            return;
        } else {
            try {
                jobStates.put(runtimeId.getConnectionId(), FeedJobState.CONGESTION_REPORTED);
                int inflowRate = message.getInflowRate();
                int outflowRate = message.getOutflowRate();
                List<String> computeLocations = new ArrayList<String>();
                computeLocations.addAll(FeedLifecycleListener.INSTANCE.getComputeLocations(runtimeId.getConnectionId()
                        .getFeedId()));
                int computeCardinality = computeLocations.size();
                int requiredCardinality = (int) Math.ceil((double) ((computeCardinality * inflowRate) / outflowRate));
                int additionalComputeNodes = requiredCardinality - computeCardinality;
                List<String> helperComputeNodes = getNodeForSubstitution(additionalComputeNodes);

                // Step 1) Alter the original feed job to adjust the cardinality
                JobSpecification jobSpec = FeedLifecycleListener.INSTANCE.getCollectJobSpecification(runtimeId
                        .getConnectionId());
                FeedUtil.alterCardinality(jobSpec, FeedRuntimeType.COMPUTE, requiredCardinality, helperComputeNodes,
                        computeLocations);

                // Step 2) send prepare to  stall message
                PrepareStallMessage stallMessage = new PrepareStallMessage(runtimeId.getConnectionId());
                String[] intakeLocations = FeedLifecycleListener.INSTANCE.getIntakeLocations(runtimeId
                        .getConnectionId().getFeedId());
                JobSpecification messageJobSpec = FeedOperations.buildPrepareStallMessageJob(stallMessage,
                        intakeLocations);
                runJob(messageJobSpec, runtimeId.getConnectionId());

                // Step 3) run the altered job specification 
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("New Job after adjusting to the workload " + jobSpec);
                }
                runJob(jobSpec, runtimeId.getConnectionId());

                jobStates.put(runtimeId.getConnectionId(), FeedJobState.UNDER_RECOVERY);
            } catch (Exception e) {
                e.printStackTrace();
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Unable to form the required job for scaling in/out" + e.getMessage());
                }
                throw new AsterixException(e);
            }
        }
    }

    private void runJob(JobSpecification spec, FeedConnectionId connectionId) throws AsterixException {
        IHyracksClientConnection hcc = AsterixAppContextInfo.getInstance().getHcc();
        try {
            hcc.startJob(spec);
        } catch (Exception e) {
            throw new AsterixException("Unable to start prepare stall job for " + connectionId + " " + e);
        }
    }

    @Override
    public void submitFeedRuntimeReport(JSONObject obj) {

    }

    private List<String> getNodeForSubstitution(int nRequired) {
        List<String> nodeIds = new ArrayList<String>();
        Iterator<NodeLoadReport> it = null;
        int nAdded = 0;
        while (nAdded < nRequired) {
            it = nodeReports.iterator();
            while (it.hasNext()) {
                nodeIds.add(it.next().getNodeId());
                nAdded++;
            }

        }
        return nodeIds;
    }

}
