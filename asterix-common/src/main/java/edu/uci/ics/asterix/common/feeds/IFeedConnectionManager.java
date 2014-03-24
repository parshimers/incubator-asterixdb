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

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IFeedConnectionManager {

    public static final long SOCKET_CONNECT_TIMEOUT = 5000;

    /**
     * Returns the executor service associated with the feed connection.
     * 
     * @param feedConnection
     * @return
     */
    public ExecutorService getFeedExecutorService(FeedConnectionId feedConnection);

    /**
     * @param feedSubscribibaleId
     * @param subscribableRuntime
     * @param runtimeType
     */
    public void registerSubscribableFeedRuntime(FeedSubscribableRuntimeId feedSubscribibaleId,
            ISubscribableRuntime subscribableRuntime, FeedRuntimeType runtimeType);

    /**
     * Allows registration of a feedRuntime.
     * 
     * @param feedRuntime
     * @throws Exception
     */
    public void registerFeedRuntime(BasicFeedRuntime feedRuntime) throws Exception;

    /**
     * Allows de-registration of a feed runtime.
     * 
     * @param feedRuntimeId
     */
    public void deRegisterFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * Obtain feed runtime corresponding to a feedRuntimeId
     * 
     * @param feedRuntimeId
     * @return
     */
    public BasicFeedRuntime getFeedRuntime(FeedRuntimeId feedRuntimeId);

    /**
     * De-register a feed
     * 
     * @param feedConnection
     * @throws IOException
     */
    void deregisterFeed(FeedConnectionId feedConnection);

    /**
     * Obtain the feed runtime manager associated with a feed.
     * 
     * @param feedConnection
     * @return
     */
    public FeedRuntimeManager getFeedRuntimeManager(FeedConnectionId feedConnection);

}
