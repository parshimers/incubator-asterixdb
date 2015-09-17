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

import java.io.FileOutputStream;

public class SIE1ReportBuilderRunner{
    String filePath = "/Users/kisskys/workspace/asterix_experiment/run-log/result-report/";
    //real data measure
    String runLogFilePath = "/Users/kisskys/workspace/asterix_experiment/run-log/8gen-balloon-AMLSMRTree-1000squery/log-1439878147037/run.log";
    
    //radom data measure
//    String runLogFilePath = "/Users/kisskys/workspace/asterix_experiment/run-log/8gen-balloon-AMLSMRTree-1000squery/randomPointGen/log-1440486666214/run.log";

    SIE1ReportBuilder sie1ADhbtree = new SIE1ReportBuilder("SpatialIndexExperiment1ADhbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1ADhvbtree = new SIE1ReportBuilder("SpatialIndexExperiment1ADhvbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1ARtree = new SIE1ReportBuilder("SpatialIndexExperiment1ARtree",
            runLogFilePath);
    SIE1ReportBuilder sie1AShbtree = new SIE1ReportBuilder("SpatialIndexExperiment1AShbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1ASif = new SIE1ReportBuilder("SpatialIndexExperiment1ASif",
            runLogFilePath);

    SIE1ReportBuilder sie1BDhbtree = new SIE1ReportBuilder("SpatialIndexExperiment1BDhbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1BDhvbtree = new SIE1ReportBuilder("SpatialIndexExperiment1BDhvbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1BRtree = new SIE1ReportBuilder("SpatialIndexExperiment1BRtree",
            runLogFilePath);
    SIE1ReportBuilder sie1BShbtree = new SIE1ReportBuilder("SpatialIndexExperiment1BShbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1BSif = new SIE1ReportBuilder("SpatialIndexExperiment1BSif",
            runLogFilePath);

    SIE1ReportBuilder sie1CDhbtree = new SIE1ReportBuilder("SpatialIndexExperiment1CDhbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1CDhvbtree = new SIE1ReportBuilder("SpatialIndexExperiment1CDhvbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1CRtree = new SIE1ReportBuilder("SpatialIndexExperiment1CRtree",
            runLogFilePath);
    SIE1ReportBuilder sie1CShbtree = new SIE1ReportBuilder("SpatialIndexExperiment1CShbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1CSif = new SIE1ReportBuilder("SpatialIndexExperiment1CSif",
            runLogFilePath);

    SIE1ReportBuilder sie1DDhbtree = new SIE1ReportBuilder("SpatialIndexExperiment1DDhbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1DDhvbtree = new SIE1ReportBuilder("SpatialIndexExperiment1DDhvbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1DRtree = new SIE1ReportBuilder("SpatialIndexExperiment1DRtree",
            runLogFilePath);
    SIE1ReportBuilder sie1DShbtree = new SIE1ReportBuilder("SpatialIndexExperiment1DShbtree",
            runLogFilePath);
    SIE1ReportBuilder sie1DSif = new SIE1ReportBuilder("SpatialIndexExperiment1DSif",
            runLogFilePath);

    StringBuilder sb = new StringBuilder();

    /**
     * generate sie1_ips.txt 
     */
    public void generateSIE1IPS() throws Exception {
        int minutes = 20;
        sb.setLength(0);
        sb.append("# sie1 ips(inserts per second) report\n");
        sb.append("# number of nodes, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("1,").append(sie1ADhbtree.get20minInsertPS(minutes)).append(",").append(sie1ADhvbtree.get20minInsertPS(minutes))
                .append(",").append(sie1ARtree.get20minInsertPS(minutes)).append(",").append(sie1AShbtree.get20minInsertPS(minutes))
                .append(",").append(sie1ASif.get20minInsertPS(minutes)).append("\n");

        sb.append("2,").append(sie1BDhbtree.get20minInsertPS(minutes)).append(",").append(sie1BDhvbtree.get20minInsertPS(minutes))
                .append(",").append(sie1BRtree.get20minInsertPS(minutes)).append(",").append(sie1BShbtree.get20minInsertPS(minutes))
                .append(",").append(sie1BSif.get20minInsertPS(minutes)).append("\n");

        sb.append("4,").append(sie1CDhbtree.get20minInsertPS(minutes)).append(",").append(sie1CDhvbtree.get20minInsertPS(minutes))
                .append(",").append(sie1CRtree.get20minInsertPS(minutes)).append(",").append(sie1CShbtree.get20minInsertPS(minutes))
                .append(",").append(sie1CSif.get20minInsertPS(minutes)).append("\n");

        sb.append("8,").append(sie1DDhbtree.get20minInsertPS(minutes)).append(",").append(sie1DDhvbtree.get20minInsertPS(minutes))
                .append(",").append(sie1DRtree.get20minInsertPS(minutes)).append(",").append(sie1DShbtree.get20minInsertPS(minutes))
                .append(",").append(sie1DSif.get20minInsertPS(minutes)).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_ips.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }
    
    public void generateInstantaneousInsertPS() throws Exception {
        for (int i = 0; i < 16; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(16 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DDhbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_8nodes_instantaneous_insert_ps_dhbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 16; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(16 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DDhvbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_8nodes_instantaneous_insert_ps_dhvbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 16; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(16 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DRtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_8nodes_instantaneous_insert_ps_rtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 16; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(16 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DShbtree.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_8nodes_instantaneous_insert_ps_shbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 16; i++) {
            sb.setLength(0);
            sb.append("# sie1 8nodes(16 dataGen) instantaneous inserts per second report\n");
            sb.append(sie1DSif.getInstantaneousInsertPS(i, false));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_8nodes_instantaneous_insert_ps_sif_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
    }
    
    public void generateIndexSize() throws Exception {
        sb.setLength(0);
        sb.append("# sie1 index size report\n");
        
        sb.append("# number of nodes, dhbtree, dhvbtree, rtree, shbtree, sif\n");
        sb.append("1,").append(sie1ADhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",").append(sie1ADhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id"))
                .append(",").append(sie1ARtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",").append(sie1AShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id"))
                .append(",").append(sie1ASif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",").append(sie1ASif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("2,").append(sie1BDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",").append(sie1BDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id"))
        .append(",").append(sie1BRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",").append(sie1BShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id"))
        .append(",").append(sie1BSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",").append(sie1BSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("4,").append(sie1CDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",").append(sie1CDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id"))
        .append(",").append(sie1CRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",").append(sie1CShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id"))
        .append(",").append(sie1CSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",").append(sie1CSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");
        sb.append("8,").append(sie1DDhbtree.getIndexSize("Tweets_idx_dhbtreeLocation/device_id")).append(",").append(sie1DDhvbtree.getIndexSize("Tweets_idx_dhvbtreeLocation/device_id"))
        .append(",").append(sie1DRtree.getIndexSize("Tweets_idx_rtreeLocation/device_id")).append(",").append(sie1DShbtree.getIndexSize("Tweets_idx_shbtreeLocation/device_id"))
        .append(",").append(sie1DSif.getIndexSize("Tweets_idx_sifLocation/device_id")).append(",").append(sie1DSif.getIndexSize("Tweets_idx_Tweets/device_id")).append("\n");

        FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_index_size.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }
    
    public void generateGanttInstantaneousInsertPS() throws Exception {
        
        SIE1ReportBuilder dhbtree = sie1ADhbtree;
        SIE1ReportBuilder dhvbtree = sie1ADhvbtree;
        SIE1ReportBuilder rtree = sie1ARtree;
        SIE1ReportBuilder shbtree = sie1AShbtree;
        SIE1ReportBuilder sif = sie1ASif;
        String sie1Type = "A";
        String logDirPrefix = "";
        
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(dhbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_instantaneous_insert_ps_dhbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(dhvbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_instantaneous_insert_ps_dhvbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(rtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_instantaneous_insert_ps_rtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(shbtree.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_instantaneous_insert_ps_shbtree_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        for (int i = 0; i < 1; i++) {
            sb.setLength(0);
            sb.append("# sie1 1node(1 dataGen) instantaneous inserts per second report\n");
            sb.append(sif.getInstantaneousInsertPS(i, true));
            FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_instantaneous_insert_ps_sif_gen"+i+".txt");
            fos.write(sb.toString().getBytes());
            ReportBuilderHelper.closeOutputFile(fos);
        }
        
        //real data 
        String parentPath = "/Users/kisskys/workspace/asterix_experiment/run-log/8gen-balloon-AMLSMRTree-1000squery/log-1439878147037/";
        
        //random data
        //String parentPath = "/Users/kisskys/workspace/asterix_experiment/run-log/8gen-balloon-AMLSMRTree-1000squery/randomPointGen/log-1440486666214/";
        long dataGenStartTime = dhbtree.getDataGenStartTimeStamp();
        NCLogReportBuilder ncLogReportBuilder = new NCLogReportBuilder(parentPath + "SpatialIndexExperiment1"+sie1Type+"Dhbtree/"+logDirPrefix+"logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        FileOutputStream fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_flush_merge_dhbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
        
        dataGenStartTime = dhvbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(parentPath + "SpatialIndexExperiment1"+sie1Type+"Dhvbtree/"+logDirPrefix+"logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_flush_merge_dhvbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
        
        dataGenStartTime = rtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(parentPath + "SpatialIndexExperiment1"+sie1Type+"Rtree/"+logDirPrefix+"logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_flush_merge_rtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
        
        dataGenStartTime = shbtree.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(parentPath + "SpatialIndexExperiment1"+sie1Type+"Shbtree/"+logDirPrefix+"logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_flush_merge_shbtree.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
        
        dataGenStartTime = sif.getDataGenStartTimeStamp();
        ncLogReportBuilder = new NCLogReportBuilder(parentPath + "SpatialIndexExperiment1"+sie1Type+"Sif/"+logDirPrefix+"logs/a1_node1.log");
        sb.setLength(0);
        sb.append(ncLogReportBuilder.getFlushMergeEventAsGanttChartFormat(dataGenStartTime));
        fos = ReportBuilderHelper.openOutputFile(filePath + "sie1_gantt_1node_flush_merge_sif.txt");
        fos.write(sb.toString().getBytes());
        ReportBuilderHelper.closeOutputFile(fos);
    }


}
