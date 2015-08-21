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

package org.apache.asterix.formats.nontagged;

import java.io.Serializable;

import org.apache.asterix.dataflow.data.nontagged.hash.AMurmurHash3BinaryHashFunctionFamily;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IBinaryHashFunctionFamilyProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

/**
 * We use a binary hash function that promotes numeric types (INT8,INT16,INT32,INT64,FLOAT) to DOUBLE if requested.
 * Non-numeric types will be hashed without type promotion.
 */
public class AqlBinaryHashFunctionFamilyProvider implements IBinaryHashFunctionFamilyProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AqlBinaryHashFunctionFamilyProvider INSTANCE = new AqlBinaryHashFunctionFamilyProvider();

    private AqlBinaryHashFunctionFamilyProvider() {

    }

    @Override
    public IBinaryHashFunctionFamily getBinaryHashFunctionFamily(Object type) throws AlgebricksException {
        // AMurmurHash3BinaryHashFunctionFamily converts numeric type to double type before doing hash()
        return AMurmurHash3BinaryHashFunctionFamily.INSTANCE;
    }

}
