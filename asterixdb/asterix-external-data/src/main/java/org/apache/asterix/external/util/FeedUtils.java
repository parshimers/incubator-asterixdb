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
package org.apache.asterix.external.util;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;
import org.apache.hyracks.api.comm.FrameHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class FeedUtils {
    private static String prepareDataverseFeedName(String dataverseName, String feedName) {
        return dataverseName + File.separator + feedName;
    }

    public static FileSplit splitsForAdapter(String dataverseName, String feedName, int partition,
            ClusterPartition[] nodePartitions) {
        File relPathFile = new File(prepareDataverseFeedName(dataverseName, feedName));
        String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
        ClusterPartition nodePartition = nodePartitions[0];
        String storagePartitionPath = StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                nodePartition.getPartitionId());
        // format: 'storage dir name'/partition_#/dataverse/feed/adapter_#
        FileReference f = new FileReference(storagePartitionPath + File.separator + relPathFile + File.separator
                + StoragePathUtil.ADAPTER_INSTANCE_PREFIX + partition);
        return StoragePathUtil.getFileSplitForClusterPartition(nodePartition, f);
    }

    public static FileSplit[] splitsForAdapter(String dataverseName, String feedName,
            AlgebricksPartitionConstraint partitionConstraints) throws AsterixException {
        if (partitionConstraints.getPartitionConstraintType() == PartitionConstraintType.COUNT) {
            throw new AsterixException("Can't create file splits for adapter with count partitioning constraints");
        }
        File relPathFile = new File(prepareDataverseFeedName(dataverseName, feedName));
        String[] locations = null;
        locations = ((AlgebricksAbsolutePartitionConstraint) partitionConstraints).getLocations();
        List<FileSplit> splits = new ArrayList<FileSplit>();
        String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
        int i = 0;
        for (String nd : locations) {
            // Always get the first partition
            ClusterPartition nodePartition = AsterixClusterProperties.INSTANCE.getNodePartitions(nd)[0];
            String storagePartitionPath = StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                    nodePartition.getPartitionId());
            // format: 'storage dir name'/partition_#/dataverse/feed/adapter_#
            FileReference f = new FileReference(storagePartitionPath + File.separator + relPathFile + File.separator
                    + StoragePathUtil.ADAPTER_INSTANCE_PREFIX + i, FileReference.FileReferenceType.DISTRIBUTED_IF_AVAIL);
            splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartition, f));
            i++;
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileReference getAbsoluteFileRef(String relativePath, int ioDeviceId, IIOManager ioManager) {
        return ioManager.getAbsoluteFileRef(ioDeviceId, relativePath);
    }

    public static FeedLogManager getFeedLogManager(IHyracksTaskContext ctx, int partition,
            FileSplit[] feedLogFileSplits) throws HyracksDataException {
        return new FeedLogManager(
                FeedUtils.getAbsoluteFileRef(feedLogFileSplits[partition].getLocalFile().getFile().getPath(),
                        feedLogFileSplits[partition].getIODeviceId(), ctx.getIOManager()).getFile());
    }

    public static FeedLogManager getFeedLogManager(IHyracksTaskContext ctx, FileSplit feedLogFileSplit)
            throws HyracksDataException {
        return new FeedLogManager(FeedUtils.getAbsoluteFileRef(feedLogFileSplit.getLocalFile().getFile().getPath(),
                feedLogFileSplit.getIODeviceId(), ctx.getIOManager()).getFile());
    }

    public static void processFeedMessage(ByteBuffer input, ByteBuffer message, FrameTupleAccessor fta) {
        // read the message and reduce the number of tuples
        fta.reset(input);
        int tc = fta.getTupleCount() - 1;
        int offset = fta.getTupleStartOffset(tc);
        int len = fta.getTupleLength(tc);
        message.clear();
        message.put(input.array(), offset, len);
        message.flip();
        IntSerDeUtils.putInt(input.array(), FrameHelper.getTupleCountOffset(input.capacity()), tc);
    }

    public static int getNumOfFields(Map<String, String> configuration) {
        return 1;
    }

    public static String getFeedMetaTypeName(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_META_TYPE_NAME);

    }
}
