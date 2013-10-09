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

package edu.uci.ics.asterix.metadata.entities;

import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

/**
 * Metadata describing a feed.
 */
public class SecondaryFeed extends Feed implements IMetadataEntity {

    private static final long serialVersionUID = 1L;

    private final String sourceFeedDataverseName;
    private final String sourceFeedName;

    public SecondaryFeed(String dataverseName, String feedName, String sourceFeedDataverse, String sourceFeedName,
            FunctionSignature appliedFunction) {
        super(dataverseName, feedName, appliedFunction, FeedType.SECONDARY);
        this.sourceFeedDataverseName = sourceFeedDataverse;
        this.sourceFeedName = feedName;
    }

    public String getSourceFeedDataverseName() {
        return sourceFeedDataverseName;
    }

    public String getSourceFeedName() {
        return sourceFeedName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!super.equals(other) || !(other instanceof SecondaryFeed)) {
            return false;
        }

        SecondaryFeed otherFeed = (SecondaryFeed) other;
        if (!(otherFeed.getSourceFeedDataverseName() == null || otherFeed.getSourceFeedDataverseName().equals(
                sourceFeedDataverseName))) {
            return false;
        }
        if (!otherFeed.getSourceFeedName().equals(sourceFeedName)) {
            return false;
        }
        return true;
    }
}