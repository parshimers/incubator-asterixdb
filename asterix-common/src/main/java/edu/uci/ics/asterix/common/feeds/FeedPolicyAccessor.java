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

public class FeedPolicyAccessor {
    public static final String FAILURE_LOG_ERROR = "failure.log.error";
    public static final String APPLICATION_FAILURE_LOG_DATA = "application.failure.log.data";
    public static final String APPLICATION_FAILURE_CONTINUE = "application.failure.continue";
    public static final String HARDWARE_FAILURE_CONTINUE = "hardware.failure.continue";
    public static final String SPILL_TO_DISK_ON_CONGESTION = "spill.to.disk.on.congestion";
    public static final String MAX_SPILL_SIZE_ON_DISK = "max.spill.size.on.disk";
    public static final String CLUSTER_REBOOT_AUTO_RESTART = "cluster.reboot.auto.restart";
    public static final String COLLECT_STATISTICS = "collect.statistics";
    public static final String COLLECT_STATISTICS_PERIOD = "collect.statistics.period";
    public static final String COLLECT_STATISTICS_PERIOD_UNIT = "collect.statistics.period.unit";
    public static final String ELASTIC = "elastic";

    public enum TimeUnit {
        SEC(1),
        MIN(60),
        HRS(3600),
        DAYS(86400);

        private int factor;

        private TimeUnit(final int value) {
            this.factor = value;
        }
    }

    private Map<String, String> feedPolicy;

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public FeedPolicyAccessor(Map<String, String> feedPolicy) {
        this.feedPolicy = feedPolicy;
    }

    public void reset(Map<String, String> feedPolicy) {
        this.feedPolicy = feedPolicy;
    }

    public boolean logErrorOnFailure() {
        return getBooleanPropertyValue(FAILURE_LOG_ERROR);
    }

    public boolean logDataOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_LOG_DATA);
    }

    public boolean continueOnApplicationFailure() {
        return getBooleanPropertyValue(APPLICATION_FAILURE_CONTINUE);
    }

    public boolean continueOnHardwareFailure() {
        return getBooleanPropertyValue(HARDWARE_FAILURE_CONTINUE);
    }

    public boolean spillToDiskOnCongestion() {
        return getBooleanPropertyValue(SPILL_TO_DISK_ON_CONGESTION);
    }

    public int getMaxSpillOnDisk() {
        return getIntegerPropertyValue(MAX_SPILL_SIZE_ON_DISK);
    }

    public boolean autoRestartOnClusterReboot() {
        return getBooleanPropertyValue(CLUSTER_REBOOT_AUTO_RESTART);
    }

    public boolean collectStatistics() {
        return getBooleanPropertyValue(COLLECT_STATISTICS);
    }

    public long getStatisicsCollectionPeriodInSecs() {
        int value = getIntegerPropertyValue(COLLECT_STATISTICS_PERIOD);
        int factor = TimeUnit.valueOf(feedPolicy.get(COLLECT_STATISTICS_PERIOD_UNIT)).factor;
        return value * factor;
    }

    public boolean isElastic() {
        return getBooleanPropertyValue(ELASTIC);
    }

    private boolean getBooleanPropertyValue(String key) {
        String v = feedPolicy.get(key);
        return v == null ? false : Boolean.valueOf(v);
    }

    private int getIntegerPropertyValue(String key) {
        String v = feedPolicy.get(key);
        return Integer.parseInt(v);
    }

}