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
package org.apache.hyracks.storage.am.lsm.common.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.frames.LIFOMetaDataFrame;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFilter;
import org.apache.hyracks.storage.common.buffercache.VirtualPage;
import org.junit.Assert;
import org.junit.Test;

public class LSMComponentFilterReferenceTest {

    @Test
    public void test() throws HyracksDataException {
        LSMComponentFilterReference filter = new LSMComponentFilterReference(
                new TypeAwareTupleWriter(new ITypeTraits[] { IntegerPointable.TYPE_TRAITS }));
        Assert.assertEquals(filter.getLength(),0);
        Assert.assertFalse(filter.isMaxTupleSet() || filter.isMinTupleSet());
        filter.writeMaxTuple(TupleUtils.createIntegerTuple(Integer.MIN_VALUE));
        Assert.assertFalse(filter.isMinTupleSet());
        Assert.assertTrue(filter.isMaxTupleSet());
        Assert.assertFalse(filter.getLength() != 11);
        filter.writeMinTuple(TupleUtils.createIntegerTuple(Integer.MIN_VALUE));
        Assert.assertTrue(filter.isMinTupleSet() && filter.isMaxTupleSet());
        Assert.assertFalse(filter.getLength() != 20);
    }
}
