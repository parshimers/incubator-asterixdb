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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.AMutableLine;
import org.apache.asterix.om.base.AMutablePoint;
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

public class CreateLineDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input type
    private static final byte SER_POINT_TYPE_TAG = ATypeTag.POINT.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreateLineDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
                    private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(outInput0);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(outInput1);
                    private AMutableLine aLine = new AMutableLine(null, null);
                    private AMutablePoint[] aPoint = { new AMutablePoint(0, 0), new AMutablePoint(0, 0) };
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ALine> lineSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ALINE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);

                        // type-check: (point, point)
                        if (outInput0.getByteArray()[0] != SER_POINT_TYPE_TAG
                                || outInput1.getByteArray()[0] != SER_POINT_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.CREATE_LINE.getName()
                                    + ": expects input type: (POINT, POINT) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                                    + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0])
                                    + ").");
                        }

                        try {
                            aPoint[0].setValue(ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aPoint[1].setValue(ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aLine.setValue(aPoint[0], aPoint[1]);
                            lineSerde.serialize(aLine, out);
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
        return AsterixBuiltinFunctions.CREATE_LINE;
    }

}