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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.dataflow.data.nontagged.Coordinate;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ACircle;
import edu.uci.ics.asterix.om.base.AMutableCircle;
import edu.uci.ics.asterix.om.base.AMutablePoint;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CreateCircleDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    private static final byte SER_DOUBLE_TYPE_TAG = ATypeTag.DOUBLE.serialize();
    private static final byte SER_POINT_TYPE_TAG = ATypeTag.POINT.serialize();

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CreateCircleDescriptor();
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
                    private AMutablePoint aPoint = new AMutablePoint(0, 0);
                    private AMutableCircle aCircle = new AMutableCircle(null, 0);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ACircle> circleSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ACIRCLE);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        outInput0.reset();
                        eval0.evaluate(tuple);
                        outInput1.reset();
                        eval1.evaluate(tuple);

                        // Type check: (point, double)
                        if (outInput0.getByteArray()[0] != SER_POINT_TYPE_TAG
                                || outInput1.getByteArray()[0] != SER_DOUBLE_TYPE_TAG) {
                            throw new AlgebricksException(AsterixBuiltinFunctions.CREATE_CIRCLE.getName()
                                    + ": expects input type (POINT, DOUBLE) but got ("
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0])
                                    + ", "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput1.getByteArray()[0])
                                    + ")");
                        }

                        try {
                            aPoint.setValue(ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                    APointSerializerDeserializer.getCoordinateOffset(Coordinate.X)),
                                    ADoubleSerializerDeserializer.getDouble(outInput0.getByteArray(),
                                            APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y)));
                            aCircle.setValue(aPoint,
                                    ADoubleSerializerDeserializer.getDouble(outInput1.getByteArray(), 1));
                            circleSerde.serialize(aCircle, out);
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
        return AsterixBuiltinFunctions.CREATE_CIRCLE;
    }

}