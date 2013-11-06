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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime;
import edu.uci.ics.asterix.common.feeds.BasicFeedRuntime.FeedRuntimeId;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedId;
import edu.uci.ics.asterix.common.feeds.FeedMessageService;
import edu.uci.ics.asterix.common.feeds.FeedRuntimeManager;
import edu.uci.ics.asterix.common.feeds.FeedSubscribableRuntimeId;
import edu.uci.ics.asterix.common.feeds.IFeedConnectionManager;
import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;
import edu.uci.ics.asterix.common.feeds.ISubscribableRuntime;
import edu.uci.ics.asterix.common.feeds.SuperFeedManager;

/**
 * An implementation of the IFeedManager interface.
 * Provider necessary central repository for registering/retrieving
 * artifacts/services associated with a feed.
 */
public class FeedConnectionManager implements IFeedConnectionManager {

    private static final Logger LOGGER = Logger.getLogger(FeedConnectionManager.class.getName());

    private Map<FeedConnectionId, FeedRuntimeManager> feedRuntimeManagers = new HashMap<FeedConnectionId, FeedRuntimeManager>();
    private final String nodeId;

    public FeedConnectionManager(String nodeId) {
        this.nodeId = nodeId;
    }

    public FeedRuntimeManager getFeedRuntimeManager(FeedConnectionId feedId) {
        return feedRuntimeManagers.get(feedId);
    }

    public ExecutorService getFeedExecutorService(FeedConnectionId feedId) {
        FeedRuntimeManager mgr = feedRuntimeManagers.get(feedId);
        return mgr == null ? null : mgr.getExecutorService();
    }

    @Override
    public FeedMessageService getFeedMessageService(FeedConnectionId feedId) {
        FeedRuntimeManager mgr = feedRuntimeManagers.get(feedId);
        return mgr == null ? null : mgr.getMessageService();
    }

    @Override
    public void deregisterFeed(FeedConnectionId feedId) {
        try {
            FeedRuntimeManager mgr = feedRuntimeManagers.get(feedId);
            if (mgr == null) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Unknown feed id: " + feedId);
                }
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Closing feed runtime manager: " + mgr);
                }
                mgr.close(true);
            }
        } catch (Exception e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Exception in closing feed runtime" + e.getMessage());
            }
            e.printStackTrace();
        }

        feedRuntimeManagers.remove(feedId);
    }

    @Override
    public void registerFeedRuntime(BasicFeedRuntime feedRuntime) throws Exception {
        FeedConnectionId feedId = feedRuntime.getFeedRuntimeId().getFeedConnectionId();
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(feedId);
        if (runtimeMgr == null) {
            synchronized (feedRuntimeManagers) {
                if (runtimeMgr == null) {
                    runtimeMgr = new FeedRuntimeManager(feedId, this);
                    feedRuntimeManagers.put(feedId, runtimeMgr);
                }
            }
        }

        runtimeMgr.registerFeedRuntime(feedRuntime.getFeedRuntimeId(), feedRuntime);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Registered runtime " + feedRuntime + " for feed " + feedId);
        }
    }

    @Override
    public void deRegisterFeedRuntime(FeedRuntimeId feedRuntimeId) {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(feedRuntimeId.getFeedConnectionId());
        if (runtimeMgr != null) {
            runtimeMgr.deregisterFeedRuntime(feedRuntimeId);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Deregistered Feed Runtime " + feedRuntimeId);
            }
        }
    }

    @Override
    public BasicFeedRuntime getFeedRuntime(FeedRuntimeId feedRuntimeId) {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(feedRuntimeId.getFeedConnectionId());
        return runtimeMgr != null ? runtimeMgr.getFeedRuntime(feedRuntimeId) : null;
    }

    @Override
    public void registerSuperFeedManager(FeedConnectionId feedId, SuperFeedManager sfm) throws Exception {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(feedId);
        if (runtimeMgr != null) {
            runtimeMgr.setSuperFeedManager(sfm);
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Registered Super Feed Manager " + sfm);
            }
        }
    }

    @Override
    public SuperFeedManager getSuperFeedManager(FeedConnectionId feedId) {
        FeedRuntimeManager runtimeMgr = feedRuntimeManagers.get(feedId);
        return runtimeMgr != null ? runtimeMgr.getSuperFeedManager() : null;
    }

    @Override
    public String toString() {
        return "FeedManager " + "[" + nodeId + "]";
    }

    @Override
    public void registerSubscribableFeedRuntime(FeedSubscribableRuntimeId feedSubscribibaleId,
            ISubscribableRuntime subscribableRuntime, FeedRuntimeType runtimeType) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deregisterComputeRuntime(FeedId feedId) {
        // TODO Auto-generated method stub

    }

}
