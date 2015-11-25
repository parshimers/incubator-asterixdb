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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.AMutableRectangle;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialCellDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialCellDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();

                    private final ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput2 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage outInput3 = new ArrayBackedValueStorage();
                    private final ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private final ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);
                    private final ICopyEvaluator eval2 = args[2].createEvaluator(outInput2);
                    private final ICopyEvaluator eval3 = args[3].createEvaluator(outInput3);
                    private final AMutableRectangle aRectangle = new AMutableRectangle(null, null);
                    private final AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };

                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<ARectangle> rectangleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ARECTANGLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);
                        outInput2.reset();
                        eval2.evaluate(tuple);
                        outInput3.reset();
                        eval3.evaluate(tuple);

                        try {
                            ATypeTag tag0 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput0.getByteArray()[0]);
                            ATypeTag tag1 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput1.getByteArray()[0]);
                            ATypeTag tag2 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput2.getByteArray()[0]);
                            ATypeTag tag3 = EnumDeserializer.ATYPETAGDESERIALIZER
                                    .deserialize(outInput3.getByteArray()[0]);
                            if (tag0 == ATypeTag.POINT && tag1 == ATypeTag.POINT && tag2 == ATypeTag.DOUBLE
                                    && tag3 == ATypeTag.DOUBLE) {
                                double xLoc = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yLoc = ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xOrigin = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
                                double yOrigin = ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                        APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));

                                double xInc = ADoubleSerializerDeserializer.getDouble(outInput2.getByteArray(), 1);
                                double yInc = ADoubleSerializerDeserializer.getDouble(outInput3.getByteArray(), 1);

                                double x = xOrigin + (Math.floor((xLoc - xOrigin) / xInc)) * xInc;
                                double y = yOrigin + (Math.floor((yLoc - yOrigin) / yInc)) * yInc;
                                aPoint[0].setValue(x, y);
                                aPoint[1].setValue(x + xInc, y + yInc);
                                aRectangle.setValue(aPoint[0], aPoint[1]);
                                rectangleSerde.serialize(aRectangle, out);
                            } else if (tag0 == ATypeTag.NULL || tag1 == ATypeTag.NULL || tag2 == ATypeTag.NULL
                                    || tag3 == ATypeTag.NULL) {
                                nullSerde.serialize(ANull.NULL, out);
                            } else {
                                throw new AlgebricksException(
                                        AsterixBuiltinFunctions.SPATIAL_CELL.getName()
                                                + ": expects input type: (POINT, POINT, DOUBLE, DOUBLE) but got ("
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0
                                                        .getByteArray()[0])
                                                + ", "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1
                                                        .getByteArray()[0])
                                                + ", "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput2
                                                        .getByteArray()[0])
                                                + ", "
                                                + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput3
                                                        .getByteArray()[0]) + ").");
                            }
                        } catch (IOException e1) {
                            throw new AlgebricksException(e1);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SPATIAL_CELL;
    }

}