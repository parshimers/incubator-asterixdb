/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.dataflow.data.nontagged.comparators;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.LongPointable;

public class AUUIDPartialBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final AUUIDPartialBinaryComparatorFactory INSTANCE = new AUUIDPartialBinaryComparatorFactory();

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                int msbCompare = Long.compare(LongPointable.getLong(b1, s1),
                        LongPointable.getLong(b2, s2));
                if (msbCompare == 0) {
                    return Long.compare(LongPointable.getLong(b1, s1 + 8),
                            LongPointable.getLong(b2, s2 + 8));
                } else {
                    return msbCompare;
                }
            }
        };
    }

}
