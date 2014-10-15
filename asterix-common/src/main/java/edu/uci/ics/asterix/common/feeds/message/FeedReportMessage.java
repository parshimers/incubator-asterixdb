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

package edu.uci.ics.asterix.common.feeds.message;

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.ValueType;

public class FeedReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private final ValueType valueType;
    private int value;

    public FeedReportMessage(FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType, int value) {
        super(MessageType.FEED_REPORT);
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.valueType = valueType;
        this.value = value;
    }

    public void reset(int value) {
        this.value = value;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.RUNTIME_TYPE, runtimeId.getFeedRuntimeType());
        obj.put(FeedConstants.MessageConstants.PARTITION, runtimeId.getPartition());
        obj.put(FeedConstants.MessageConstants.VALUE_TYPE, valueType);
        obj.put(FeedConstants.MessageConstants.VALUE, value);
        return obj;
    }

}
