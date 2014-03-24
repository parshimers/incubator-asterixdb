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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;

public class FeedRuntimeManager {

    private static Logger LOGGER = Logger.getLogger(FeedRuntimeManager.class.getName());

    private final FeedConnectionId feedId;
    private final IFeedConnectionManager feedConnectionManager;
    private final Map<FeedRuntimeId, BasicFeedRuntime> feedRuntimes;

    private final ExecutorService executorService;
    private final LinkedBlockingQueue<String> feedReportQueue;

    public FeedRuntimeManager(FeedConnectionId feedId, IFeedConnectionManager feedConnectionManager) {
        this.feedId = feedId;
        feedRuntimes = new ConcurrentHashMap<FeedRuntimeId, BasicFeedRuntime>();
        executorService = Executors.newCachedThreadPool();
        feedReportQueue = new LinkedBlockingQueue<String>();
        this.feedConnectionManager = feedConnectionManager;
    }

    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdownNow();
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Shut down executor service for :" + feedId);
            }
        }
    }

    public BasicFeedRuntime getFeedRuntime(FeedRuntimeId runtimeId) {
        return feedRuntimes.get(runtimeId);
    }

    public void registerFeedRuntime(FeedRuntimeId runtimeId, BasicFeedRuntime feedRuntime) {
        feedRuntimes.put(runtimeId, feedRuntime);
    }

    public void deregisterFeedRuntime(FeedRuntimeId runtimeId) {
        feedRuntimes.remove(runtimeId);
        if (feedRuntimes.isEmpty()) {
            synchronized (this) {
                if (feedRuntimes.isEmpty()) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("De-registering feed");
                    }
                    feedConnectionManager.deregisterFeed(runtimeId.getFeedConnectionId());
                }
            }
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public FeedConnectionId getFeedId() {
        return feedId;
    }

    public LinkedBlockingQueue<String> getFeedReportQueue() {
        return feedReportQueue;
    }

}
