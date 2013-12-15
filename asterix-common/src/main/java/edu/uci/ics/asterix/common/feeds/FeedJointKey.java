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
package edu.uci.ics.asterix.common.feeds;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class FeedJointKey {

    private final FeedId primaryFeedId;
    private final List<String> appliedFunctions;
    private final String stringRep;

    public FeedJointKey(FeedId feedId, List<String> appliedFunctions) {
        this.primaryFeedId = feedId;
        this.appliedFunctions = appliedFunctions;
        StringBuilder builder = new StringBuilder();
        builder.append(feedId);
        builder.append(":");
        builder.append(StringUtils.join(appliedFunctions, ':'));
        stringRep = builder.toString();
    }

    public FeedId getFeedId() {
        return primaryFeedId;
    }

    public List<String> getAppliedFunctions() {
        return appliedFunctions;
    }

    public String getStringRep() {
        return stringRep;
    }

    @Override
    public final String toString() {
        return stringRep;
    }

    @Override
    public int hashCode() {
        return stringRep.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FeedJointKey)) {
            return false;
        }

        return stringRep.equals(((FeedJointKey) o).stringRep);
    }

}