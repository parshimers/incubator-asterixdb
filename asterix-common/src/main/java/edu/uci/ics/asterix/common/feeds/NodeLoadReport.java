/*
 * Copyright 2009-2014 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.feeds;

public class NodeLoadReport implements Comparable<NodeLoadReport> {

    private final String nodeId;

    private final float avgCpuLoad;

    public NodeLoadReport(String nodeId, float avgCpuLoad) {
        this.nodeId = nodeId;
        this.avgCpuLoad = avgCpuLoad;
    }

    public String getNodeId() {
        return nodeId;
    }

    public float getAvgCpuLoad() {
        return avgCpuLoad;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof NodeLoadReport)) {
            return false;
        }
        return ((NodeLoadReport) o).nodeId.equals(nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public int compareTo(NodeLoadReport o) {
        return (int) (this.avgCpuLoad - ((NodeLoadReport) o).avgCpuLoad);
    }
}
