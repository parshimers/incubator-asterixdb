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

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.storage.am.common.api.ILinearizerSearchHelper;
import org.apache.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;

public class HilbertBTreeSearchHelper implements ILinearizerSearchHelper {

    private ISerializerDeserializer<APoint> pointSerde;
    private AMutablePoint aPoint; 
    private ISerializerDeserializer<AInt64> int64Serde;
    private AMutableInt64 aInt64; 
    private final double qBottomLeftX;
    private final double qBottomLeftY;
    private final double qTopRightX;
    private final double qTopRightY;
    private PermutingFrameTupleReference queryRegion; //for debugging

    @SuppressWarnings("unchecked")
    public HilbertBTreeSearchHelper(PermutingFrameTupleReference queryRegion) throws HyracksDataException {
        aPoint = new AMutablePoint(0, 0);
        pointSerde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.APOINT);
        aInt64 = new AMutableInt64(0);
        int64Serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
        
        int fieldStartOffset = queryRegion.getFieldStart(0);
        qBottomLeftX = ADoubleSerializerDeserializer.getDouble(queryRegion.getFieldData(0), fieldStartOffset
                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
        qBottomLeftY = ADoubleSerializerDeserializer.getDouble(queryRegion.getFieldData(0), fieldStartOffset
                + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
        qTopRightX = ADoubleSerializerDeserializer.getDouble(queryRegion.getFieldData(0), fieldStartOffset
                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
        qTopRightY = ADoubleSerializerDeserializer.getDouble(queryRegion.getFieldData(0), fieldStartOffset
                + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));
        this.queryRegion = queryRegion;
    }

    @Override
    public double getQueryBottomLeftX() {
        return qBottomLeftX;
    }

    @Override
    public double getQueryBottomLeftY() {
        return qBottomLeftY;
    }

    @Override
    public double getQueryTopRightX() {
        return qTopRightX;
    }
    
    @Override
    public double getQueryTopRightY() {
        return qTopRightY;
    }

    @Override
    public void convertPointField2TwoDoubles(byte[] bytes, int fieldStartOffset, double[] out)
            throws HyracksDataException {
        //deserialize a point field in the byte array
        out[0] = ADoubleSerializerDeserializer.getDouble(bytes,
                fieldStartOffset + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
        out[1] = ADoubleSerializerDeserializer.getDouble(bytes,
                fieldStartOffset + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
    }

    @Override
    public void convertTwoDoubles2PointField(double[] in, ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        aPoint.setValue(in[0], in[1]);
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        pointSerde.serialize(aPoint, dos);
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public void convertLong2Int64Field(long in, ArrayTupleBuilder tupleBuilder) throws HyracksDataException {
        aInt64.setValue(in);
        DataOutput dos = tupleBuilder.getDataOutput();
        tupleBuilder.reset();
        int64Serde.serialize(aInt64, dos);
        tupleBuilder.addFieldEndOffset();
    }

    @Override
    public long convertInt64Field2Long(byte[] bytes, int fieldStartOffset) throws HyracksDataException {
        return AInt64SerializerDeserializer.getLong(bytes, fieldStartOffset+1);
    }
}
