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

public class SIE2ReportBuilder extends AbstractDynamicDataEvalReportBuilder {
    private static final int SELECT_QUERY_RADIUS_COUNT = 5;
    private static final int INITIAL_SELECT_QUERY_COUNT_TO_CONSIDER = 100;
    private static final int MAX_SELECT_QUERY_COUNT_TO_CONSIDER = 1000 + INITIAL_SELECT_QUERY_COUNT_TO_CONSIDER;


    public SIE2ReportBuilder(String expName, String runLogFilePath) {
        super(expName, runLogFilePath);
    }

    @Override
    public String getOverallInsertPS() throws Exception {
        return null;
    }

    @Override
    public String get20minInsertPS(int minutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long insertCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("[During ingestion + queries][InsertCount]")) {
                    insertCount += ReportBuilderHelper.getLong(line, "=", "in");
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(insertCount/(minutes * 60));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public double getFirstXminInsertPS(int minutes, int genId, int unitMinutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return 0;
            }

            String line;
            int dGenId;
            int count = 0;
            long timeToInsert = 0;
            long totalTimeToInsert = 0;
            boolean haveResult = false;
            while ((line = br.readLine()) != null) {
                if (line.contains("[During ingestion only][TimeToInsert100000]")) {
                    dGenId = ReportBuilderHelper.getInt(line, "DataGen[", "]");
                    if (dGenId == genId) {
                        count++;
                        timeToInsert = ReportBuilderHelper.getLong(line, INSTANTANEOUS_INSERT_STRING, "in");
                        totalTimeToInsert += timeToInsert;
                        if (totalTimeToInsert > minutes*60000) {
                            haveResult = true;
                            break;
                        }
                    }
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            if (haveResult || totalTimeToInsert > (minutes*60000 - unitMinutes*60000)) {
                return  (count * INSTANTAEOUS_INSERT_COUNT) / ((double)totalTimeToInsert/1000);
            } else {
                return 0;
                //return  ((count * INSTANTAEOUS_INSERT_COUNT) / ((double)totalTimeToInsert/1000)) * -1;
            }
        } finally {
            closeRunLog();
        }
    }

    @Override
    public String getInstantaneousQueryPS() throws Exception {
        return null;
    }

    @Override
    public String get20minQueryPS(int minutes) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long queryCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("[QueryCount]")) {
                    queryCount += ReportBuilderHelper.getLong(line, "[QueryCount]", "in");
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(queryCount/(float)(minutes * 60));
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String get20minAverageQueryResultCount() throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long resultCount = 0;
            long queryCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("i64")) {
                    resultCount += ReportBuilderHelper.getLong(line, "[", "i64");
                    ++queryCount;
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(resultCount/queryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String get20minAverageQueryResponseTime() throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long responseTime = 0;
            long queryCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("Elapsed time = ")) {
                    responseTime += ReportBuilderHelper.getLong(line, "=", "for");
                    ++queryCount;
                }
                if (line.contains("Running")) {
                    break;
                }
            }
            rsb.append(responseTime/queryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String getSelectQueryResponseTime(int radiusIdx) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long queryResponseTime = 0;
            int selectQueryCount = 0;
            int targetRadiusSelectQueryCount = 0;
            int queryGenCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("i64")) {
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        line = br.readLine();
                        if (line.contains("Elapsed time =") && selectQueryCount < MAX_SELECT_QUERY_COUNT_TO_CONSIDER) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx && selectQueryCount >= INITIAL_SELECT_QUERY_COUNT_TO_CONSIDER) {
                                queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (line.contains("[QueryCount]")) {
                            ++queryGenCount;
                            selectQueryCount = 0;
                            break;
                        }
                    }
                    if (queryGenCount == 8) {
                        break;
                    }
                }
            }
            rsb.append((double)queryResponseTime / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String getSelectQueryResultCount(int radiusIdx) throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long queryResultCount = 0;
            int selectQueryCount = 0;
            int targetRadiusSelectQueryCount = 0;
            int queryGenCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("i64")) {
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        if (line.contains("i64") && selectQueryCount < MAX_SELECT_QUERY_COUNT_TO_CONSIDER) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx && selectQueryCount >= INITIAL_SELECT_QUERY_COUNT_TO_CONSIDER) {
                                queryResultCount += ReportBuilderHelper.getLong(line, "[", "i64");
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }

                        if (line.contains("[QueryCount]")) {
                            ++queryGenCount;
                            selectQueryCount = 0;
                            break;
                        }
                        line = br.readLine();
                    }
                    if (queryGenCount == 8) {
                        break;
                    }
                }
            }
            rsb.append((double)queryResultCount / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
}
