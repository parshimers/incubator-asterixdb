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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;

public class FeedMetricCollector implements IFeedMetricCollector {

    private static final Logger LOGGER = Logger.getLogger(FeedMetricCollector.class.getName());

    private final String nodeId;

    private final AtomicInteger senderId = new AtomicInteger(1);

    private final Map<Integer, Sender> senders = new HashMap<Integer, Sender>();

    private final Map<Integer, Series> statHistory = new HashMap<Integer, Series>();

    private static final long METRIC_ANALYSIS_PERIODICITY = 1000; // 1 second

    private final Timer timer;

    private static final int UNKNOWN = -1;

    private Map<String, Sender> sendersByName;

    public FeedMetricCollector(String nodeId) {
        this.nodeId = nodeId;
        sendersByName = new HashMap<String, Sender>();
        this.timer = new Timer();
        //timer.scheduleAtFixedRate(new ProcessCollectedStats(), 0, METRIC_ANALYSIS_PERIODICITY);
    }

    @Override
    public synchronized int createReportSender(FeedConnectionId connectionId, FeedRuntimeId runtimeId,
            ValueType valueType, MetricType metricType) {
        Sender sender = new Sender(senderId.getAndIncrement(), connectionId, runtimeId, valueType, metricType);
        senders.put(sender.senderId, sender);
        sendersByName.put(sender.getDisplayName(), sender);
        return sender.senderId;

    }

    @Override
    public void removeReportSender(int senderId) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            statHistory.remove(senderId);
            senders.remove(senderId);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to remove sender Id");
            }
        }
    }

    @Override
    public String toString() {
        return "FeedMetricCollector" + " [" + nodeId + "]";
    }

    @Override
    public void sendReport(int senderId, int value) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            Series series = statHistory.get(sender.senderId);
            if (series == null) {
                series = new Series();
                statHistory.put(sender.senderId, series);
            }
            series.addValue(value);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to report for sender Id" + senderId + " " + value);
            }
        }
    }

    @Override
    public void resetReportSender(int senderId) {
        Sender sender = senders.get(senderId);
        if (sender != null) {
            Series series = statHistory.get(sender.senderId);
            series.reset();
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Sender with id " + senderId + " not found. Unable to reset!");
            }
        }
    }

    private static class Sender {

        private final int senderId;
        private final MetricType mType;
        private final String displayName;

        public Sender(int senderId, FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType,
                MetricType mType) {
            this.senderId = senderId;
            this.mType = mType;
            this.displayName = createDisplayName(connectionId, runtimeId, valueType);
        }

        @Override
        public String toString() {
            return displayName + "[" + senderId + "]" + "(" + mType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Sender)) {
                return false;
            }
            return ((Sender) o).senderId == senderId;
        }

        @Override
        public int hashCode() {
            return senderId;
        }

        public static String createDisplayName(FeedConnectionId connectionId, FeedRuntimeId runtimeId,
                ValueType valueType) {
            return connectionId + " (" + runtimeId.getFeedRuntimeType() + " )" + "[" + runtimeId.getPartition() + "]"
                    + "{" + valueType + "}";
        }

        public String getDisplayName() {
            return displayName;
        }

        public int getSenderId() {
            return senderId;
        }
    }

    private class Series {
        private long windowBegin;
        private long windowEnd;
        private int currentValue;
        private int nEntries;

        public Series() {
            this.windowBegin = -1;
            this.windowEnd = -1;
            this.currentValue = 0;
            this.nEntries = 0;
        }

        public synchronized void addValue(int value) {
            if (windowBegin < 0) {
                markBeginning(); // ignore the first entry
            } else {
                currentValue += value;
                nEntries++;
                windowEnd = System.currentTimeMillis();
            }
        }

        public synchronized void markBeginning() {
            windowBegin = System.currentTimeMillis();
            currentValue = 0;
            nEntries = 0;
            windowEnd = -1;
        }

        public synchronized float getAvg() {
            int sum = getSum();
            return ((float) sum) / nEntries;
        }

        public synchronized int getSum() {
            return currentValue;
        }

        public synchronized float getRate() {
            if (nEntries < 0) { // first window
                return UNKNOWN;
            }
            int sum = currentValue;
            long timeElapsed = windowEnd - windowBegin;
            float result = ((float) (sum * 1000) / timeElapsed);
            return result;
        }

        public int getSize() {
            return nEntries;
        }

        public void reset() {
            this.windowBegin = -1;
            this.windowEnd = -1;
            this.currentValue = 0;
            this.nEntries = 0;
        }
    }

    private class ProcessCollectedStats extends TimerTask {

        private final StringBuilder report = new StringBuilder();

        @Override
        public void run() {
            int result = -1;
            Date d = new Date();
            for (Entry<Integer, Sender> entry : senders.entrySet()) {
                Sender sender = entry.getValue();
                Series series = statHistory.get(sender.senderId);
                try {
                    result = getMetric(sender.senderId);
                } catch (Exception e) {
                    e.printStackTrace();
                    result = UNKNOWN;
                }
                if (result == UNKNOWN) {
                    continue;
                }
                series.markBeginning();
                report.append(d.toString() + sender.displayName + ": " + result + " " + sender.mType + "\n");
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info(report.toString());
                }
                report.delete(0, report.length() - 1);
            }
        }
    }

    @Override
    public int getMetric(int senderId) {
        Sender sender = senders.get(senderId);
        return getMetric(sender);
    }

    @Override
    public int getMetric(FeedConnectionId connectionId, FeedRuntimeId runtimeId, ValueType valueType) {
        String displayName = Sender.createDisplayName(connectionId, runtimeId, valueType);
        Sender sender = sendersByName.get(displayName);
        return getMetric(sender);
    }

    private int getMetric(Sender sender) {
        if (sender == null || statHistory.get(sender.getSenderId()) == null
                || statHistory.get(sender.getSenderId()).getSize() == 0) {
            return UNKNOWN;
        }

        float result = -1;
        Series series = statHistory.get(sender.getSenderId());
        switch (sender.mType) {
            case AVG:
                result = series.getAvg();
                break;
            case RATE:
                result = series.getRate();
                break;
        }
        return (int) result;
    }

}
