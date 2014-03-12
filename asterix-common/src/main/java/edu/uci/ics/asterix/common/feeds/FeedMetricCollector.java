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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class FeedMetricCollector implements IFeedMetricCollector {

    private static final Logger LOGGER = Logger.getLogger(FeedMetricCollector.class.getName());

    private final String nodeId;

    private final AtomicInteger senderId = new AtomicInteger(1);

    private final Map<Integer, Sender> senders = new HashMap<Integer, Sender>();

    private final Map<Integer, Series> statHistory = new HashMap<Integer, Series>();

    private static final long DEFAULT_PERIODICITY = 1000;
    private static final int MAX_HISTORY_SIZE = 10000;

    private final Timer timer;

    public FeedMetricCollector(String nodeId) {
        this.nodeId = nodeId;
        this.timer = new Timer();
        timer.scheduleAtFixedRate(new ProcessCollectedStats(), 0, DEFAULT_PERIODICITY);
    }

    @Override
    public String toString() {
        return "FeedMetricCollector" + " [" + nodeId + "]";
    }

    @Override
    public int createReportSender(String displayName, MetricType metricType) {
        Sender sender = new Sender(senderId.getAndIncrement(), displayName, metricType);
        senders.put(sender.senderId, sender);
        return sender.senderId;
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
            System.out.println("Unable to report for sender Id" + senderId + " " + value);
        }
    }

    private static class Sender {
        private final int senderId;
        private final MetricType mType;
        private final String displayName;

        public Sender(int senderId, String displayName, MetricType mType) {
            this.senderId = senderId;
            this.mType = mType;
            this.displayName = displayName;
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

        public synchronized float getRateNew() {
            float result = 0;
            long timeElapsed = windowBegin < 0 ? DEFAULT_PERIODICITY : (System.currentTimeMillis() - windowBegin);
            result = ((float) (currentValue * 1000) / timeElapsed);
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
            float result = -1;
            boolean dataCollected = false;
            Date d = new Date();
            for (Entry<Integer, Sender> entry : senders.entrySet()) {
                Sender sender = entry.getValue();
                Series series = statHistory.get(sender.senderId);
                if (series == null || series.getSize() == 0) {
                    continue;
                }
                dataCollected = true;
                switch (sender.mType) {
                    case AVG:
                        result = series.getAvg();
                        break;
                    case RATE:
                        result = series.getRate();
                        break;
                }
                series.reset();
                report.append(d.toString() + sender.displayName + ": " + result + " " + sender.mType + "\n");
                System.out.println(report.toString());
                report.delete(0, report.length() - 1);
            }
        }
    }

}
