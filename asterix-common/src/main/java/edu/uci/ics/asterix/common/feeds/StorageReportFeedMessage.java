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

/**
 * A feed control message sent from a storage runtime of a feed pipeline to report the intake timestamp corresponding
 * to the last persisted tuple.
 */
public class StorageReportFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId connectionId;
    private final int partition;

    private long lastPersistedTupleIntakeTimestamp;

    public StorageReportFeedMessage(FeedConnectionId connectionId, int partition, long lastPersistedTupleIntakeTimestamp) {
        super(MessageType.STORAGE_REPORT);
        this.connectionId = connectionId;
        this.partition = partition;
        this.lastPersistedTupleIntakeTimestamp = lastPersistedTupleIntakeTimestamp;
    }

    @Override
    public String toString() {
        return messageType.name() + " " + connectionId + " [" + lastPersistedTupleIntakeTimestamp + "] ";
    }

    public FeedConnectionId getConnectionId() {
        return connectionId;
    }

    public long getLastPersistedTupleIntakeTimestamp() {
        return lastPersistedTupleIntakeTimestamp;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put(FeedConstants.MessageConstants.MESSAGE_TYPE, messageType.name());
        obj.put(FeedConstants.MessageConstants.DATAVERSE, connectionId.getFeedId().getDataverse());
        obj.put(FeedConstants.MessageConstants.FEED, connectionId.getFeedId().getFeedName());
        obj.put(FeedConstants.MessageConstants.DATASET, connectionId.getDatasetName());
        obj.put(FeedConstants.MessageConstants.LAST_PERSISTED_TUPLE_INTAKE_TIMESTAMP, lastPersistedTupleIntakeTimestamp);
        obj.put(FeedConstants.MessageConstants.PARTITION, partition);
        return obj;
    }

    public static StorageReportFeedMessage read(JSONObject obj) throws JSONException {
        FeedId feedId = new FeedId(obj.getString(FeedConstants.MessageConstants.DATAVERSE),
                obj.getString(FeedConstants.MessageConstants.FEED));
        FeedConnectionId connectionId = new FeedConnectionId(feedId, obj.getString(FeedConstants.MessageConstants.DATASET));
        int partition = obj.getInt(FeedConstants.MessageConstants.PARTITION);
        long timestamp = obj.getLong(FeedConstants.MessageConstants.LAST_PERSISTED_TUPLE_INTAKE_TIMESTAMP);
        return new StorageReportFeedMessage(connectionId, partition, timestamp);
    }

    public void reset(long lastPersistedTupleIntakeTimestamp) {
        this.lastPersistedTupleIntakeTimestamp = lastPersistedTupleIntakeTimestamp;
    }

}
