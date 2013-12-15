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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class FeedSubscriber {

    private static final Logger LOGGER = Logger.getLogger(FeedSubscriber.class.getName());

    private final FeedConnectionId feedConnectionId;

    private FeedConnectionInfo feedConnectionInfo;

    public enum Status {
        CREATED,
        INIITIALIZED,
        ACTIVE,
        INACTIVE
    }

    private Status status;

    private final String feedPolicy;

    private final Map<String, String> feedPolicyParameters;

    private JobId jobId;

    private JobSpecification jobSpec;

    private final FeedJointKey sourceFeedPointKey;

    private String superFeedManagerHost;

    private int superFeedManagerPort;

    public FeedSubscriber(FeedJointKey sourceFeedPointKey, FeedConnectionId feedConnectionId, String feedPolicy,
            Map<String, String> feedPolicyParameters, Status status) {
        this.sourceFeedPointKey = sourceFeedPointKey;
        this.feedConnectionId = feedConnectionId;
        this.status = status;
        this.feedPolicy = feedPolicy;
        this.feedPolicyParameters = feedPolicyParameters;
    }

    @Override
    public int hashCode() {
        return feedConnectionId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FeedSubscriber)) {
            return false;
        }
        return ((FeedSubscriber) o).feedConnectionId.equals(feedConnectionId);
    }

    @Override
    public String toString() {
        return feedConnectionId.toString() + "[" + status + "]";
    }

    public FeedConnectionId getFeedConnectionId() {
        return feedConnectionId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("FeedSubscriber " + "[" + feedConnectionId + "]" + " is now " + status);
        }
    }

    public Map<String, String> getFeedPolicyParameters() {
        return feedPolicyParameters;
    }

    public String getFeedPolicy() {
        return feedPolicy;
    }

    public JobId getJobId() {
        return jobId;
    }

    public void setJobId(JobId jobId) {
        this.jobId = jobId;
    }

    public FeedConnectionInfo getFeedConnectionInfo() {
        return feedConnectionInfo;
    }

    public void setFeedConnectionInfo(FeedConnectionInfo feedConnectionInfo) {
        this.feedConnectionInfo = feedConnectionInfo;
    }

    public String getSuperFeedManagerHost() {
        return superFeedManagerHost;
    }

    public void setSuperFeedManagerHost(String superFeedManagerHost) {
        this.superFeedManagerHost = superFeedManagerHost;
    }

    public int getSuperFeedManagerPort() {
        return superFeedManagerPort;
    }

    public void setSuperFeedManagerPort(int superFeedManagerPort) {
        this.superFeedManagerPort = superFeedManagerPort;
    }

    public JobSpecification getJobSpec() {
        return jobSpec;
    }

    public void setJobSpec(JobSpecification jobSpec) {
        this.jobSpec = jobSpec;
    }

    public FeedJointKey getSourceFeedPointKey() {
        return sourceFeedPointKey;
    }
}
