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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ILinearizerSearchHelper;
import org.apache.hyracks.storage.am.common.api.ILinearizerSearchPredicate;
import org.apache.hyracks.storage.am.common.ophelpers.MultiComparator;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class HilbertBTreeRangePredicate extends RangePredicate implements ILinearizerSearchPredicate {
    private static final long serialVersionUID = 1L;

    private PermutingFrameTupleReference queryRegion;
    private final LinearizerSearchComparisonType comparisonType = LinearizerSearchComparisonType.HILBERT_ORDER_BASED_RELATIVE_COMPARISON_BETWEETN_TWO_OBJECTS;

    public HilbertBTreeRangePredicate(ITupleReference lowKey, ITupleReference highKey, boolean lowKeyInclusive,
            boolean highKeyInclusive, MultiComparator lowKeyCmp, MultiComparator highKeyCmp,
            PermutingFrameTupleReference minFilterKey, PermutingFrameTupleReference maxFilterKey,
            PermutingFrameTupleReference queryRegion) {
        super(lowKey, highKey, lowKeyInclusive, highKeyInclusive, lowKeyCmp, highKeyCmp, minFilterKey, maxFilterKey);
        this.queryRegion = queryRegion;
    }

    @Override
    public ILinearizerSearchHelper getLinearizerSearchHelper() throws HyracksDataException {
        return new HilbertBTreeSearchHelper(queryRegion);
    }

    @Override
    public LinearizerSearchComparisonType getComparisonType() {
        return comparisonType;
    }
}
