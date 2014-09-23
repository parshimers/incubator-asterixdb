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

import edu.uci.ics.asterix.common.config.AsterixFeedProperties;

/**
 * Handle (de)registration of feeds for delivery of control messages.
 */
public interface IFeedManager {

    public static final long SOCKET_CONNECT_TIMEOUT = 5000;

    public String getNodeId();

    public IFeedSubscriptionManager getFeedSubscriptionManager();

    public IFeedConnectionManager getFeedConnectionManager();

    public IFeedMemoryManager getFeedMemoryManager();

    public IFeedMetadataManager getFeedMetadataManager();

    public IFeedMetricCollector getFeedMetricCollector();

    public IFeedMessageService getFeedMessageService();

    public AsterixFeedProperties getAsterixFeedProperties();

}
