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

public class SIE3ReportBuilder extends AbstractDynamicDataEvalReportBuilder {
    private static final int WARM_UP_QUERY_COUNT = 500;
    private static final int SELECT_QUERY_COUNT = 5000;
    private static final int JOIN_QUERY_COUNT = 200;
    private static final int SELECT_QUERY_RADIUS_COUNT = 5; //0.00001, 0.0001, 0.001, 0.01, 0.1
    private static final int JOIN_QUERY_RADIUS_COUNT = 4; ////0.00001, 0.0001, 0.001, 0.01
    
    public SIE3ReportBuilder(String expName, String runLogFilePath) {
        super(expName, runLogFilePath);
    }

    @Override
    public String getOverallInsertPS() throws Exception {
        return null;
    }

    @Override
    public String get20minInsertPS(int minutes) throws Exception {
        return null;
    }

    @Override
    public String getInstantaneousQueryPS() throws Exception {
        return null;
    }

    @Override
    public String get20minQueryPS(int minutes) throws Exception {
        return null;
    }
    
    public String getIndexCreationTime() throws Exception {
        renewStringBuilder();
        openRunLog();
        try {
            if (!moveToExperimentBegin()) {
                //The experiment run log doesn't exist in this run log file
                return null;
            }
            
            String line;
            long indexCreationTime = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("There is no index with this name")) {
                    indexCreationTime += ReportBuilderHelper.getLong(line, "=", "for");
                    break;
                }
            }
            rsb.append((double)indexCreationTime/(1000*60));
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
            while((line = br.readLine()) != null) {
                if (line.contains("800000000i64")) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT queries' results 
                    while(true) {
                        line = br.readLine();
                        if (line.contains("Elapsed time =")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT) {
                            break;
                        }
                    }
                    
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        line = br.readLine();
                        if (line.contains("Elapsed time =")) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx) {
                                queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
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
            while((line = br.readLine()) != null) {
                if (line.contains("800000000i64")) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT queries' results 
                    while(true) {
                        line = br.readLine();
                        if (line.contains("i64")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT) {
                            break;
                        }
                    }
                    
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        line = br.readLine();
                        if (line.contains("i64")) {
                            if (selectQueryCount % SELECT_QUERY_RADIUS_COUNT == radiusIdx) {
                                System.out.println("radiusIdx["+ radiusIdx +"] : " + ReportBuilderHelper.getLong(line, "[", "i64"));
                                queryResultCount += ReportBuilderHelper.getLong(line, "[", "i64");
                                ++targetRadiusSelectQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double)queryResultCount / targetRadiusSelectQueryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String getJoinQueryResponseTime(int radiusIdx) throws Exception {
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
            int targetRadiusJoinQueryCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("800000000i64")) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT queries' results 
                    while(true) {
                        line = br.readLine();
                        if (line.contains("Elapsed time =")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT+SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    
                    selectQueryCount = 0;
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        line = br.readLine();
                        if (line.contains("Elapsed time =")) {
                            if (selectQueryCount % JOIN_QUERY_RADIUS_COUNT == radiusIdx) {
                                queryResponseTime += ReportBuilderHelper.getLong(line, "=", "for");
                                ++targetRadiusJoinQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == JOIN_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double)queryResponseTime / targetRadiusJoinQueryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
    
    public String getJoinQueryResultCount(int radiusIdx) throws Exception {
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
            int targetRadiusJoinQueryCount = 0;
            while((line = br.readLine()) != null) {
                if (line.contains("800000000i64")) {
                    //select queries start after total count query
                    // read and discard WARM_UP_QUERY_COUNT + SELECT_QUERY_COUNT queries' results 
                    while(true) {
                        line = br.readLine();
                        if (line.contains("i64")) {
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == WARM_UP_QUERY_COUNT+SELECT_QUERY_COUNT) {
                            break;
                        }
                    }
                    
                    selectQueryCount = 0;
                    // read and calculate the average query response time for the requested(target) radius
                    while(true) {
                        line = br.readLine();
                        if (line.contains("i64")) {
                            if (selectQueryCount % JOIN_QUERY_RADIUS_COUNT == radiusIdx) {
                                queryResultCount += ReportBuilderHelper.getLong(line, "[", "i64");
                                ++targetRadiusJoinQueryCount;
                            }
                            ++selectQueryCount;
                        }
                        if (selectQueryCount == JOIN_QUERY_COUNT) {
                            break;
                        }
                    }
                    break;
                }
            }
            rsb.append((double)queryResultCount / targetRadiusJoinQueryCount);
            return rsb.toString();
        } finally {
            closeRunLog();
        }
    }
}
