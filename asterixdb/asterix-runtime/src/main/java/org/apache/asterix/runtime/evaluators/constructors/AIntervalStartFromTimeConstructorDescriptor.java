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
package org.apache.asterix.runtime.evaluators.constructors;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableDuration;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.base.temporal.ADurationParserFactory;
import org.apache.asterix.om.base.temporal.ATimeParserFactory;
import org.apache.asterix.om.base.temporal.DurationArithmeticOperations;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.base.temporal.ADurationParserFactory.ADurationParseOption;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.InvalidDataFormatException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class AIntervalStartFromTimeConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = BuiltinFunctions.INTERVAL_CONSTRUCTOR_START_FROM_TIME;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new AIntervalStartFromTimeConstructorDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private IPointable argPtr0 = new VoidPointable();
                    private IPointable argPtr1 = new VoidPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);

                    private AMutableInterval aInterval = new AMutableInterval(0L, 0L, (byte) 0);
                    private AMutableDuration aDuration = new AMutableDuration(0, 0L);
                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<AInterval> intervalSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINTERVAL);
                    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        byte[] bytes0 = argPtr0.getByteArray();
                        int offset0 = argPtr0.getStartOffset();
                        int len0 = argPtr0.getLength();
                        byte[] bytes1 = argPtr1.getByteArray();
                        int offset1 = argPtr1.getStartOffset();
                        int len1 = argPtr1.getLength();

                        try {
                            long intervalStart = 0, intervalEnd = 0;
                            if (bytes0[offset0] == ATypeTag.SERIALIZED_TIME_TYPE_TAG) {
                                intervalStart = ATimeSerializerDeserializer.getChronon(bytes0, offset0 + 1);
                            } else if (bytes0[offset0] == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                utf8Ptr.set(bytes0, offset0 + 1, len0 - 1);

                                int stringLength = utf8Ptr.getUTF8Length();

                                intervalStart = ATimeParserFactory.parseTimePart(bytes0, utf8Ptr.getCharStartOffset(),
                                        stringLength);
                            } else {
                                throw new TypeMismatchException(getIdentifier(), 0, bytes0[offset0],
                                        ATypeTag.SERIALIZED_TIME_TYPE_TAG, ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }

                            if (intervalStart < 0) {
                                intervalStart += GregorianCalendarSystem.CHRONON_OF_DAY;
                            }

                            if (bytes1[offset1] == ATypeTag.SERIALIZED_DURATION_TYPE_TAG) {
                                if (ADurationSerializerDeserializer.getYearMonth(bytes1, offset1 + 1) != 0) {
                                    throw new InvalidDataFormatException(getIdentifier(),
                                            ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                                }

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        ADurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1), false);
                            } else if (bytes1[offset1] == ATypeTag.SERIALIZED_DAY_TIME_DURATION_TYPE_TAG) {
                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        ADayTimeDurationSerializerDeserializer.getDayTime(bytes1, offset1 + 1), false);
                            } else if (bytes1[offset1] == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
                                // duration
                                utf8Ptr.set(bytes1, offset1 + 1, len1 - 1);
                                int stringLength = utf8Ptr.getUTF8Length();
                                ADurationParserFactory.parseDuration(bytes1, utf8Ptr.getCharStartOffset(), stringLength,
                                        aDuration, ADurationParseOption.All);
                                if (aDuration.getMonths() != 0) {
                                    throw new InvalidDataFormatException(getIdentifier(),
                                            ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                                }

                                intervalEnd = DurationArithmeticOperations.addDuration(intervalStart, 0,
                                        aDuration.getMilliseconds(), false);
                            } else {
                                throw new TypeMismatchException(getIdentifier(), 1, bytes1[offset1],
                                        ATypeTag.SERIALIZED_DURATION_TYPE_TAG,
                                        ATypeTag.SERIALIZED_DAY_TIME_DURATION_TYPE_TAG,
                                        ATypeTag.SERIALIZED_STRING_TYPE_TAG);
                            }

                            if (intervalEnd > GregorianCalendarSystem.CHRONON_OF_DAY) {
                                intervalEnd = intervalEnd % (int) (GregorianCalendarSystem.CHRONON_OF_DAY);
                            }

                            if (intervalEnd < intervalStart) {
                                throw new InvalidDataFormatException(getIdentifier(),
                                        ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                            }

                            aInterval.setValue(intervalStart, intervalEnd, ATypeTag.SERIALIZED_TIME_TYPE_TAG);
                            intervalSerde.serialize(aInterval, out);
                        } catch (IOException e) {
                            throw new InvalidDataFormatException(getIdentifier(), e,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
