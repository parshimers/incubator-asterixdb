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
package edu.uci.ics.asterix.metadata.cluster;

import edu.uci.ics.asterix.metadata.api.IClusterEventsSubscriber;

public class AddNodeWork extends AbstractClusterManagementWork {

    private final int numberOfNodes;

    @Override
    public WorkType getClusterManagementWorkType() {
        return WorkType.ADD_NODE;
    }

    public AddNodeWork(int numberOfNodes, IClusterEventsSubscriber subscriber) {
        super(subscriber);
        this.numberOfNodes = numberOfNodes;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    @Override
    public String toString() {
        return WorkType.ADD_NODE + " " + numberOfNodes + " requested by " + subscriber;
    }
}
