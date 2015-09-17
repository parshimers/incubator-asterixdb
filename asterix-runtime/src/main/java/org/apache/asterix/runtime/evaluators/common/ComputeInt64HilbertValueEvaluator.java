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

package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.linearizer.GeoCoordinates2HilbertValueConverter;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ComputeInt64HilbertValueEvaluator implements ICopyEvaluator {
    private final DataOutput out;
    private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    private final ICopyEvaluator pointEval;
    private final GeoCoordinates2HilbertValueConverter hilbertConverter;
    private final AMutableInt64 amInt64 = new AMutableInt64(0);
    private final ISerializerDeserializer int64Serde;

    public ComputeInt64HilbertValueEvaluator(ICopyEvaluatorFactory pointEvalFactory, IDataOutputProvider output)
            throws AlgebricksException {
        out = output.getDataOutput();
        pointEval = pointEvalFactory.createEvaluator(argOut);
        hilbertConverter = new GeoCoordinates2HilbertValueConverter();
        int64Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();
        pointEval.evaluate(tuple);
        byte[] bytes = argOut.getByteArray();

        //APoint type field: [APOINTTypeTag (1byte) | double (8bytes) | double (8bytes)]   
        double x = ADoubleSerializerDeserializer.getDouble(bytes, 1);
        double y = ADoubleSerializerDeserializer.getDouble(bytes, 9);

        //compute hilbert value
        long hilbertValue = hilbertConverter.computeInt64HilbertValue(x, y);

        //output
        amInt64.setValue(hilbertValue);
        try {
            int64Serde.serialize(amInt64, out);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }
}
