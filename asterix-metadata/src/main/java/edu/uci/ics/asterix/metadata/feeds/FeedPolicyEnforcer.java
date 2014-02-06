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

import java.rmi.RemoteException;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.feeds.FeedConnectionId;
import edu.uci.ics.asterix.common.feeds.FeedPolicyAccessor;

public class FeedPolicyEnforcer {

    private final FeedConnectionId feedConnectionId;
    private final FeedPolicyAccessor feedPolicyAccessor;

    public FeedPolicyEnforcer(FeedConnectionId feedConnectionId, Map<String, String> feedPolicy) {
        this.feedConnectionId = feedConnectionId;
        this.feedPolicyAccessor = new FeedPolicyAccessor(feedPolicy);
    }

    public boolean continueIngestionPostSoftwareFailure(Exception e) throws RemoteException, ACIDException {
        boolean continueIngestion = feedPolicyAccessor.continueOnApplicationFailure();
        if (feedPolicyAccessor.logErrorOnFailure()) {
            persistExceptionDetails(e);
        }
        return continueIngestion;
    }

    private synchronized void persistExceptionDetails(Exception e) throws RemoteException, ACIDException {
        //TODO Put log message in feed shadow dataset
    }

    public FeedPolicyAccessor getFeedPolicyAccessor() {
        return feedPolicyAccessor;
    }

    public FeedConnectionId getFeedId() {
        return feedConnectionId;
    }

}
