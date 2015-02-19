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
package edu.uci.ics.asterix.test.runtime;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;

/**
 * Manages a Mini (local VM) YARN cluster with a configured number of NodeManager(s).
 * 
 */
@SuppressWarnings("deprecation")
public class YARNCluster {

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final int nameNodePort = 31888;
    private static final String DATA_PATH = "data/hdfs";
    private static final String HDFS_PATH = "/asterix";
    private static final YARNCluster INSTANCE = new YARNCluster();

    private MiniYARNCluster miniCluster;
    private MiniDFSCluster dfsCluster;
    private int numDataNodes = 2;
    private Configuration conf = new YarnConfiguration();
    private FileSystem dfs;

    public static YARNCluster getInstance() {
        return INSTANCE;
    }

    private YARNCluster() {

    }

    /**
     * Instantiates the (Mini) DFS Cluster with the configured number of datanodes.
     * Post instantiation, data is laoded to HDFS.
     * Called prior to running the Runtime test suite.
     */
    public void setup() throws Exception {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        conf.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
        cleanupLocal();
        dfsCluster = new MiniDFSCluster.Builder(conf).nameNodePort(nameNodePort).numDataNodes(numDataNodes).startupOption(StartupOption.REGULAR).build();
        dfs = dfsCluster.getFileSystem();
        conf.set("fs.defaultFS", dfs.getUri().toString());
        conf.set("fs.default.name", dfs.getUri().toString());
        //this constructor is deprecated in hadoop 2x 
        //dfsCluster = new MiniDFSCluster(nameNodePort, conf, numDataNodes, true, true, StartupOption.REGULAR, null);
        miniCluster = new MiniYARNCluster("Asterix - testing", numDataNodes, 1, 1);
        dfsCluster.formatDataNodeDirs();
        dfsCluster.waitActive();
        miniCluster.init(conf);
        loadData();
    }
    
    public MiniYARNCluster getCluster(){
        return miniCluster;
    }

    private void loadData() throws IOException {
        Path destDir = new Path(HDFS_PATH);
        dfs.mkdirs(destDir);
        File srcDir = new File(DATA_PATH);
        File[] listOfFiles = srcDir.listFiles();
        for (File srcFile : listOfFiles) {
            Path path = new Path(srcFile.getAbsolutePath());
            dfs.copyFromLocalFile(path, destDir);
        }
    }

    private void cleanupLocal() throws IOException {
        // cleanup artifacts created on the local file system
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
    }

    public void cleanup() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
            cleanupLocal();
        }
    }

}
