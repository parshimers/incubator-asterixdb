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

/**
 * A control message that can be sent to the runtime instance of a
 * feed's adapter.
 */
public abstract class FeedMessage implements IFeedMessage {

    private static final long serialVersionUID = 1L;

    protected final MessageType messageType;
    protected final FeedConnectionId connectionId;

    public FeedMessage(MessageType messageType, FeedConnectionId connectionId) {
        this.messageType = messageType;
        this.connectionId = connectionId;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public FeedConnectionId getFeedConnectionId() {
        return connectionId;
    }

    @Override
    public String toString() {
        return messageType + "[" + connectionId + "]";
    }

}
