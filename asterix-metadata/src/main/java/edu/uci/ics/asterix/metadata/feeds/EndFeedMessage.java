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
package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedMessage;
import edu.uci.ics.asterix.common.feeds.IFeedMessage;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;

/**
 * A feed control message indicating the need to end the feed. This message is dispatched
 * to all locations that host an operator involved in the feed pipeline.
 */
public class EndFeedMessage extends FeedMessage {

    private static final long serialVersionUID = 1L;

    private final FeedConnectionId feedConnectionId;

    private final FeedId sourceFeedId;

    private final FeedRuntimeType sourceRuntimeType;

    private final boolean completeDisconnection;

    private final EndMessageType endMessageType;

    public enum EndMessageType {
        DISCONNECT_FEED,
        DISCONTINUE_SOURCE
    }

    public EndFeedMessage(FeedConnectionId feedId, FeedRuntimeType sourceRuntimeType, FeedId sourceFeedId,
            boolean completeDisconnection, EndMessageType endMessageType) {
        super(MessageType.END, feedId);
        this.feedConnectionId = feedId;
        this.sourceRuntimeType = sourceRuntimeType;
        this.sourceFeedId = sourceFeedId;
        this.completeDisconnection = completeDisconnection;
        this.endMessageType = endMessageType;
    }

    @Override
    public String toString() {
        return MessageType.END.name() + feedConnectionId + " [" + sourceRuntimeType + "] ";
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public FeedRuntimeType getSourceRuntimeType() {
        return sourceRuntimeType;
    }

    public FeedId getSourceFeedId() {
        return sourceFeedId;
    }

    public boolean isCompleteDisconnection() {
        return completeDisconnection;
    }

    public EndMessageType getEndMessageType() {
        return endMessageType;
    }

    @Override
    public String toJSON() {
        return null;
    }

}
