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
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.NodeLoadReport;
import edu.uci.ics.asterix.common.feeds.api.IFeedLoadManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedLoadManager implements IFeedLoadManager {

    private final TreeSet<NodeLoadReport> nodeReports;

    public FeedLoadManager() {
        nodeReports = new TreeSet<NodeLoadReport>();
    }

    @Override
    public JobSpecification reEvaluateJob(JobSpecification jobSpec) {
        return null;
    }

    @Override
    public void submitNodeLoadReport(NodeLoadReport report) {
        nodeReports.remove(report);
        nodeReports.add(report);
    }

    @Override
    public void reportCongestion(JSONObject report) throws JSONException {
        int inflowRate = report.getInt(IFeedMessage.Constants.INFLOW_RATE);
        int outflowRate = report.getInt(IFeedMessage.Constants.OUTFLOW_RATE);
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
