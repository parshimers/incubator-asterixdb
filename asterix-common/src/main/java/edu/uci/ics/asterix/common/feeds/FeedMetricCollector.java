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

import edu.uci.ics.asterix.common.feeds.IFeedRuntime.FeedRuntimeType;

public class FeedMetricCollector implements IFeedMetricCollector {

    private static final Logger LOGGER = Logger.getLogger(FeedMetricCollector.class.getName());

    private final String nodeId;

    private final AtomicInteger senderId = new AtomicInteger(1);

    private final Map<Integer, Sender> senders = new HashMap<Integer, Sender>();

    private final Map<Integer, Series> statHistory = new HashMap<Integer, Series>();

    private static final long DEFAULT_PERIODICITY = 1000; // 1 second

    private final Timer timer;

    private static final int UNKNOWN = -1;

    private Map<String, Sender> sendersByName;

    public FeedMetricCollector(String nodeId) {
        this.nodeId = nodeId;
        sendersByName = new HashMap<String, Sender>();
        this.timer = new Timer();
        timer.scheduleAtFixedRate(new ProcessCollectedStats(), 0, DEFAULT_PERIODICITY);
    }

    @Override
    public int createReportSender(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition,
            ValueType valueType, MetricType metricType) {
        Sender sender = new Sender(senderId.getAndIncrement(), connectionId, runtimeType, partition, valueType,
                metricType);
        senders.put(sender.senderId, sender);
        sendersByName.put(sender.getDisplayName(), sender);
        return sender.senderId;

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

    private static class Sender {

        private final int senderId;
        private final MetricType mType;
        private final String displayName;

        public Sender(int senderId, FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition,
                ValueType valueType, MetricType mType) {
            this.senderId = senderId;
            this.mType = mType;
            this.displayName = createDisplayName(connectionId, runtimeType, partition, valueType);
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

        public static String createDisplayName(FeedConnectionId connectionId, FeedRuntimeType runtimeType,
                int partition, ValueType valueType) {
            return connectionId + " (" + runtimeType + " )" + "[" + partition + "]" + "{" + valueType + "}";
        }

        public String getDisplayName() {
            return displayName;
        }
    }

    private class Series {
        private long windowBegin;
        private int currentValue;
        private int nEntries;

        public Series() {
            this.windowBegin = -1;
            currentValue = 0;
            nEntries = 0;
        }

        public synchronized void addValue(int value) {
            currentValue += value;
            nEntries++;
        }

        public synchronized void reset() {
            windowBegin = System.currentTimeMillis();
            currentValue = 0;
            nEntries = 0;
        }

        public synchronized float getAvg() {
            int sum = getSum();
            return ((float) sum) / nEntries;
        }

        public synchronized int getSum() {
            int sum = currentValue;
            reset();
            return sum;
        }

        public synchronized float getRate() {
            if (windowBegin < 0) { // first window
                currentValue = 0;
                windowBegin = System.currentTimeMillis();
            }
            int sum = currentValue;
            long timeElapsed = System.currentTimeMillis() - windowBegin;
            float result = ((float) (sum * 1000) / timeElapsed);
            reset();
            return result;
        }

        public int getSize() {
            return nEntries;
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
                series.reset();
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
    public int getMetric(FeedConnectionId connectionId, FeedRuntimeType runtimeType, int partition, ValueType valueType) {
        String displayName = Sender.createDisplayName(connectionId, runtimeType, partition, valueType);
        Sender sender = sendersByName.get(displayName);
        return getMetric(sender);
    }

    private int getMetric(Sender sender) {
        if (sender == null || statHistory.get(senderId) == null || statHistory.get(senderId).getSize() == 0) {
            return UNKNOWN;
        }

        float result = -1;
        Series series = statHistory.get(senderId);
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
