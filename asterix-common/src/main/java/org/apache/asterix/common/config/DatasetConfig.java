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

package org.apache.asterix.common.config;

import java.io.Serializable;

public class DatasetConfig {

    /*
     * We have two kinds of datasets. INTERNAL: A dataset with data persisted
     * in ASTERIX storage. The dataset is populated either using a load
     * statement or using insert statement. EXTERNAL: A dataset with data
     * residing outside ASTERIX. As such ASTERIX does not maintain any indexes
     * on the data. The data for the dataset is fetched as and when required
     * from an external data source using an adapter.
     */
    public enum DatasetType {
        INTERNAL,
        EXTERNAL
    };

    public enum IndexType {
        BTREE,
        RTREE,
        SINGLE_PARTITION_WORD_INVIX,
        SINGLE_PARTITION_NGRAM_INVIX,
        LENGTH_PARTITIONED_WORD_INVIX,
        LENGTH_PARTITIONED_NGRAM_INVIX,
        SIF,
        STATIC_HILBERT_BTREE,
        DYNAMIC_HILBERT_BTREE,
        DYNAMIC_HILBERTVALUE_BTREE
    };

    public enum ExternalDatasetTransactionState {
        COMMIT, // The committed state <- nothing is required->
        BEGIN, // The state after starting the refresh transaction <- will either abort moving to committed state or move to ready to commit->
        READY_TO_COMMIT // The transaction is ready to commit <- can only move forward to committed state-> 
    };

    public enum ExternalFilePendingOp {
        PENDING_NO_OP, // the stored file is part of a committed transaction nothing is required
        PENDING_ADD_OP, // the stored file is part of an ongoing transaction (will be added if transaction succeed)
        PENDING_DROP_OP, // the stored file is part of an ongoing transaction (will be dropped if transaction succeed)
        PENDING_APPEND_OP // the stored file is part of an ongoing transaction (will be updated if transaction succeed)
    };

    public static class IndexTypeProperty implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final int CELL_BASED_SPATIAL_INDEX_MAX_LEVEL = 6;
        public static final int CELL_BASED_SPATIAL_INDEX_MAX_CELLS_PER_OBJECT = 256;
        public static final int CELL_BASED_SPATIAL_INDEX_MIN_CELLS_PER_OBJECT = 8192;
        public static final int CELL_BASED_SPATIAL_INDEX_DEFAULT_CELLS_PER_OBJECT = 1024;
        public static final short CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_LOW = 4;
        public static final short CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_MEDIUM = 16;
        public static final short CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH = 256;

        // Specific to NGram indexes.
        public int gramLength;

        // Specific to spatial indexes such as rtree, shbtree, dhbtree, and sif. 
        public double bottomLeftX;
        public double bottomLeftY;
        public double topRightX;
        public double topRightY;

        // Specific to cell-based spatial indexes such as shbtree and sif. 
        public short[] levelDensity = new short[] { CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH,
                CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH, CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH,
                CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH, CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH,
                CELL_BASED_SPATIAL_INDEX_CELL_DENSITY_HIGH };
        public int cellsPerObject = CELL_BASED_SPATIAL_INDEX_DEFAULT_CELLS_PER_OBJECT;
    }
}
