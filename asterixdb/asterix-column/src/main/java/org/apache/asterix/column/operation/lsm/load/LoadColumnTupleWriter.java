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
package org.apache.asterix.column.operation.lsm.load;

import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnTupleWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

public class LoadColumnTupleWriter extends FlushColumnTupleWriter {
    public LoadColumnTupleWriter(FlushColumnMetadata columnMetadata, int pageSize, int maxNumberOfTuples,
            double tolerance, int maxLeafNodeSize) {
        super(columnMetadata, pageSize, maxNumberOfTuples, tolerance, maxLeafNodeSize);
    }

    @Override
    public void writeTuple(ITupleReference tuple) throws HyracksDataException {
        writeRecord(tuple);
    }
}
