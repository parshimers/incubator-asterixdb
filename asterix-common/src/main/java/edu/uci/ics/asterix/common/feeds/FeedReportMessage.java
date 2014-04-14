package edu.uci.ics.asterix.common.feeds;

import org.json.JSONException;
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
import org.json.JSONObject;

import edu.uci.ics.asterix.common.feeds.IFeedMetricCollector.ValueType;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;

public class FeedReportMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedRuntimeType runtimeType;
    private final int partition;
    private final ValueType valueType;
    private int value;

    public FeedReportMessage(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition,
            ValueType valueType, int value) {
        super(MessageType.REPORT, connectionId);
        this.runtimeType = runtimeType;
        this.partition = partition;
        this.valueType = valueType;
        this.value = value;
    }

    public void reset(int value) {
        this.value = value;
    }

    @Override
    public String toJSON() throws JSONException {
        JSONObject obj = new JSONObject();
        obj.put("message-type", messageType.name());
        obj.put("dataverse", connectionId.getFeedId().getDataverse());
        obj.put("feed", connectionId.getFeedId().getFeedName());
        obj.put("dataset", connectionId.getDatasetName());
        obj.put("runtimeType", runtimeType);
        obj.put("partition", partition);
        obj.put("valueType", valueType);
        obj.put("value", value);
        return obj.toString();
    }

}
