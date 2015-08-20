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

import java.util.Set;

import edu.uci.ics.asterix.common.api.IClusterEventsSubscriber;

public class RemoveNodeWork extends AbstractClusterManagementWork {

    private final Set<String> nodesToBeRemoved;

    @Override
    public WorkType getClusterManagementWorkType() {
        return WorkType.REMOVE_NODE;
    }

    public RemoveNodeWork(Set<String> nodesToBeRemoved, IClusterEventsSubscriber subscriber) {
        super(subscriber);
        this.nodesToBeRemoved = nodesToBeRemoved;
    }

    public Set<String> getNodesToBeRemoved() {
        return nodesToBeRemoved;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(WorkType.REMOVE_NODE);
        for (String node : nodesToBeRemoved) {
            builder.append(node + " ");
        }
        builder.append(" requested by " + subscriber);
        return builder.toString();
    }

}
