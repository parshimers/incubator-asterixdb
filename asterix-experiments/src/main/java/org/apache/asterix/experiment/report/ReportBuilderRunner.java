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

public class ReportBuilderRunner {

    public static void main(String[] args) throws Exception {
        SIE1ReportBuilderRunner sie1 = new SIE1ReportBuilderRunner();
        sie1.generateSIE1IPS();
        sie1.generateInstantaneousInsertPS();
        sie1.generateIndexSize();
        sie1.generateGanttInstantaneousInsertPS();
        
        SIE2ReportBuilderRunner sie2 = new SIE2ReportBuilderRunner();
        sie2.generate20MinInsertPS();
        sie2.generateFirst20MinInsertPS();
        sie2.generateEvery20MinInsertPS();
        sie2.generate20MinQueryPS();
        sie2.generate20MinAverageQueryResultCount();
        sie2.generate20MinAverageQueryResponseTime();
        sie2.generateInstantaneousInsertPS();
        sie2.generateGanttInstantaneousInsertPS();
        sie2.generateSelectQueryResponseTime();
        sie2.generateSelectQueryResultCount();
        
        SIE3ReportBuilderRunner sie3 = new SIE3ReportBuilderRunner();
        sie3.generateIndexCreationTime();
        sie3.generateIndexSize();
        sie3.generateSelectQueryResponseTime();
        sie3.generateJoinQueryResponseTime();
        sie3.generateSelectQueryResultCount();
        sie3.generateJoinQueryResultCount();
        sie3.generateSelectQueryProfiledSidxSearchTime();
        sie3.generateSelectQueryProfiledPidxSearchTime();
        sie3.generateJoinQueryProfiledSidxSearchTime();
        sie3.generateJoinQueryProfiledPidxSearchTime();
        sie3.generateJoinQueryProfiledSeedPidxSearchTime();
        sie3.generateSelectQueryProfiledSidxCacheMiss();
        sie3.generateSelectQueryProfiledPidxCacheMiss();
        sie3.generateJoinQueryProfiledSidxCacheMiss();
        sie3.generateJoinQueryProfiledPidxCacheMiss();
        sie3.generateJoinQueryProfiledSeedPidxCacheMiss();
        sie3.generateSelectQueryProfiledFalsePositive();
        sie3.generateJoinQueryProfiledFalsePositive();
    }

}
