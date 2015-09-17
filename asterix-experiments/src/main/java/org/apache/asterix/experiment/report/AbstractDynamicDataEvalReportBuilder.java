/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.experiment.report;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;

public abstract class AbstractDynamicDataEvalReportBuilder implements IDynamicDataEvalReportBuilder {

    protected final static String INSTANTANEOUS_INSERT_STRING = "[TimeToInsert100000]";
    protected final static int INSTANTAEOUS_INSERT_COUNT = 100000;
    protected final String expName;
    protected final String runLogFilePath;
    protected BufferedReader br = null;

    protected final StringBuilder dataGenSb;
    protected final StringBuilder queryGenSb;
    protected final StringBuilder rsb;

    protected AbstractDynamicDataEvalReportBuilder(String expName, String runLogFilePath) {
        this.expName = expName;
        this.runLogFilePath = runLogFilePath;
        dataGenSb = new StringBuilder();
        queryGenSb = new StringBuilder();
        rsb = new StringBuilder();
    }

    protected void openRunLog() throws IOException {
        br = new BufferedReader(new FileReader(runLogFilePath));
    }

    protected void closeRunLog() throws IOException {
        if (br != null) {
            br.close();
        }
    }

    protected boolean moveToExperimentBegin() throws IOException {
        String line;
        while ((line = br.readLine()) != null) {
            if (line.contains("Running experiment: " + expName)) {
                return true;
            }
        }
        return false;
    }

    protected void renewStringBuilder() {
        dataGenSb.setLength(0);
        queryGenSb.setLength(0);
        rsb.setLength(0);
    }

    @Override
    public String getInstantaneousInsertPS(int genId, boolean useTimeForX) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            int dGenId;
            int count = 0;
            long timeToInsert = 0;
            long totalTimeToInsert = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains(INSTANTANEOUS_INSERT_STRING)) {
                    dGenId = ReportBuilderHelper.getInt(line, "DataGen[", "]");
                    if (dGenId == genId) {
                        count++;
                        timeToInsert = ReportBuilderHelper.getLong(line, INSTANTANEOUS_INSERT_STRING, "in");
                        totalTimeToInsert += timeToInsert;
                        if (useTimeForX) {
                            dataGenSb.append(totalTimeToInsert/1000).append(",")
                                .append(INSTANTAEOUS_INSERT_COUNT / ((double) (timeToInsert) / 1000)).append("\n");
                        } else {
                            dataGenSb.append(count).append(",")
                                .append(INSTANTAEOUS_INSERT_COUNT / ((double) (timeToInsert) / 1000)).append("\n");
                        }
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            System.out.println("GenId[" + genId + "] " + totalTimeToInsert + ", " + (totalTimeToInsert/(1000*60)));
            return dataGenSb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public long getDataGenStartTimeStamp() throws Exception {
        openRunLog();
        try {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("Running experiment: " + expName)) {
                    while ((line = br.readLine()) != null) {
                        //2015-07-10 01:52:03,658 INFO  [ParallelActionThread 0] direct.SessionChannel (SessionChannel.java:exec(120)) - Will request to exec `JAVA_HOME=/home/youngsk2/jdk1.7.0_65/ /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/bin/datagenrunner -si 1 -of /mnt/data/sdb/youngsk2/data/simple-gps-points-120312.txt -p 0 -d 1200 128.195.9.23:10001`
                        if (line.contains("Will request to exec `JAVA_HOME=/home/youngsk2/jdk1.7.0_65/ /scratch/youngsk2/spatial-index-experiment/ingestion-experiment-root/ingestion-experiment-binary-and-configs/bin/datagenrunner")) {
                            //format1 = new SimpleDateFormat("MMM dd, yyyy hh:mm:ss aa");
                            //format2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            return ReportBuilderHelper.getTimeStampAsLong(line, format);
                        }
                    }
                }
            }
            return -1;
        } finally {
            closeRunLog();
        }
    }
    
    public String getIndexSize(String indexDirPath) throws Exception {
        /*
         * exmaple
         * /mnt/data/sdb/youngsk2/asterix/storage/experiments/Tweets_idx_dhbtreeLocation/device_id_0:
        total 211200
        -rw-r--r-- 1 youngsk2 grad 191234048 Jun 29 00:11 2015-06-29-00-09-59-023_2015-06-28-23-51-56-984_b
        -rw-r--r-- 1 youngsk2 grad   7864320 Jun 29 00:11 2015-06-29-00-09-59-023_2015-06-28-23-51-56-984_f
        -rw-r--r-- 1 youngsk2 grad   4194304 Jun 29 00:10 2015-06-29-00-10-26-997_2015-06-29-00-10-26-997_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:10 2015-06-29-00-10-26-997_2015-06-29-00-10-26-997_f
        -rw-r--r-- 1 youngsk2 grad   5898240 Jun 29 00:11 2015-06-29-00-10-59-791_2015-06-29-00-10-59-791_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:11 2015-06-29-00-10-59-791_2015-06-29-00-10-59-791_f
        -rw-r--r-- 1 youngsk2 grad   5898240 Jun 29 00:11 2015-06-29-00-11-30-486_2015-06-29-00-11-30-486_b
        -rw-r--r-- 1 youngsk2 grad    393216 Jun 29 00:11 2015-06-29-00-11-30-486_2015-06-29-00-11-30-486_f
        
         */
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }

            String line;
            String[] tokens;
            long diskSize = 0;
            while ((line = br.readLine()) != null) {
                if (line.contains(indexDirPath)) {
                    br.readLine();//discard "total XXXX" line
                    //read and sum file size
                    while (!(line = br.readLine()).isEmpty()) {
                        tokens = line.split("\\s+");;
                        diskSize += Long.parseLong(tokens[4].trim());
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append((double)diskSize /(1024*1024*1024));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
}
