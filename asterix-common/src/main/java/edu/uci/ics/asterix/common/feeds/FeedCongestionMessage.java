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

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.api.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.api.IFeedRuntime.FeedRuntimeType;

public class FeedCongestionMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final FeedRuntimeId runtimeId;
    private int inflowRate;
    private int outflowRate;

    public FeedCongestionMessage(FeedConnectionId connectionId, FeedRuntimeId runtimeId, int inflowRate, int outflowRate) {
        super(MessageType.CONGESTION);
        this.connectionId = connectionId;
        this.runtimeId = runtimeId;
        this.inflowRate = inflowRate;
        this.outflowRate = outflowRate;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(IFeedMessage.Constants.MESSAGE_TYPE, messageType.name());
        obj.put(IFeedMessage.Constants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(IFeedMessage.Constants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(IFeedMessage.Constants.DATASET, connectionId.getDatasetName());
        obj.put(IFeedMessage.Constants.RUNTIME_TYPE, runtimeId.getFeedRuntimeType());
        obj.put(IFeedMessage.Constants.OPERAND_ID, runtimeId.getOperandId());
        obj.put(IFeedMessage.Constants.PARTITION, runtimeId.getPartition());
        obj.put(IFeedMessage.Constants.INFLOW_RATE, inflowRate);
        obj.put(IFeedMessage.Constants.OUTFLOW_RATE, outflowRate);
        return obj;
    }

    public FeedRuntimeId getRuntimeId() {
        return runtimeId;
    }

    public int getInflowRate() {
        return inflowRate;
    }

    public int getOutflowRate() {
        return outflowRate;
    }

    public static FeedCongestionMessage read(JSONObject obj) throws JSONException {
        FeedId feedId = new FeedId(obj.getString(IFeedMessage.Constants.DATAVERSE),
                obj.getString(IFeedMessage.Constants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId, obj.getString(IFeedMessage.Constants.DATASET));
        FeedRuntimeId runtimeId = new FeedRuntimeId(FeedRuntimeType.valueOf(obj
                .getString(IFeedMessage.Constants.RUNTIME_TYPE)), obj.getInt(IFeedMessage.Constants.PARTITION),
                obj.getString(IFeedMessage.Constants.OPERAND_ID));
        return new FeedCongestionMessage(connectionId, runtimeId, obj.getInt(IFeedMessage.Constants.INFLOW_RATE),
                obj.getInt(IFeedMessage.Constants.OUTFLOW_RATE));
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

}
