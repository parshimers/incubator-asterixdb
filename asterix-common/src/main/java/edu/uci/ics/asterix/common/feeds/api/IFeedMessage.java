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
package edu.uci.ics.asterix.common.feeds.api;

import java.io.Serializable;

import org.json.JSONException;
import org.json.JSONObject;

public interface IFeedMessage extends Serializable {

    public enum MessageType {
        END,
        XAQL,
        FEED_REPORT,
        NODE_REPORT,
        CONGESTION
    }

    public static final class Constants {
        public static final String MESSAGE_TYPE = "message-type";
        public static final String DATAVERSE = "dataverse";
        public static final String FEED = "feed";
        public static final String DATASET = "dataset";
        public static final String AQL = "aql";
        public static final String RUNTIME_TYPE = "runtime-type";
        public static final String PARTITION = "partition";
        public static final String INFLOW_RATE = "inflow-rate";
        public static final String OUTFLOW_RATE = "outflow-rate";
        public static final String VALUE_TYPE = "value-type";
        public static final String VALUE = "value";
        public static final String CPU_LOAD = "cpu-load";
        public static final String N_RUNTIMES = "n_runtimes";
        public static final String HEAP_USAGE = "heap_usage";
    }

    public MessageType getMessageType();

    public JSONObject toJSON() throws JSONException;

}
