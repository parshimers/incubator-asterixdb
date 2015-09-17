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

package org.apache.asterix.runtime.linearizer;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ISearchPredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class HilbertValueBTreeSearchOperatorNodePushable extends IndexSearchOperatorNodePushable {

    protected PermutingFrameTupleReference queryRegion;
    protected final boolean btreeLowKeyInclusive;
    protected final boolean btreeHighKeyInclusive;
    protected ArrayTupleReference btreeLowKey;
    protected ArrayTupleReference btreeHighKey;
    protected MultiComparator btreeLowKeyCmp;
    protected MultiComparator btreeHighKeyCmp;
    protected ArrayTupleBuilder btreeKeyBuilder;
    protected ArrayTupleReference btreeKeyReference;

    protected ByteBuffer btreeKeyFrame;
    private HilbertValueBTreeRangePredicate hilbertValueBTreeRangePredicate;

    public HilbertValueBTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, int[] lowKeyFields,
            int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive, int[] minFilterFieldIndexes,
            int[] maxFilterFieldIndexes) throws HyracksDataException {
        super(opDesc, ctx, partition, recordDescProvider, minFilterFieldIndexes, maxFilterFieldIndexes);
        queryRegion = new PermutingFrameTupleReference();
        queryRegion.setFieldPermutation(lowKeyFields);
        //Currently, HilbertValueBTree always has an int64 type key field in the first field.
        btreeLowKey = new ArrayTupleReference();
        btreeLowKey.reset(new int[] { 0 }, null);
        btreeHighKey = null;
        this.btreeLowKeyInclusive = true;
        this.btreeHighKeyInclusive = true;
        btreeKeyBuilder = new ArrayTupleBuilder(1);

        //set initial search key to a first Hilbert value: 0
        AInt64 initialSearchKey = new AInt64(0L);
        ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT64);
        DataOutput dos = btreeKeyBuilder.getDataOutput();
        btreeKeyBuilder.reset();
        serde.serialize(initialSearchKey, dos);
        btreeKeyBuilder.addFieldEndOffset();
        btreeLowKey.reset(btreeKeyBuilder.getFieldEndOffsets(), btreeKeyBuilder.getByteArray());
    }

    @Override
    protected ISearchPredicate createSearchPredicate() throws HyracksDataException {
        ITreeIndex treeIndex = (ITreeIndex) index;
        btreeLowKeyCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), btreeLowKey);
        btreeHighKeyCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), btreeHighKey);
        hilbertValueBTreeRangePredicate = new HilbertValueBTreeRangePredicate(btreeLowKey, btreeHighKey,
                btreeLowKeyInclusive, btreeHighKeyInclusive, btreeLowKeyCmp, btreeHighKeyCmp, minFilterKey,
                maxFilterKey, queryRegion);
        return hilbertValueBTreeRangePredicate;
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) throws HyracksDataException {
        queryRegion.reset(accessor, tupleIndex);

        if (minFilterKey != null) {
            minFilterKey.reset(accessor, tupleIndex);
        }
        if (maxFilterKey != null) {
            maxFilterKey.reset(accessor, tupleIndex);
        }
    }

    @Override
    protected int getFieldCount() {
        return ((ITreeIndex) index).getFieldCount();
    }
}