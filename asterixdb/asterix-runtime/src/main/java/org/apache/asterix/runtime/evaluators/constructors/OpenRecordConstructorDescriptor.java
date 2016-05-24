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

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class OpenRecordConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new OpenRecordConstructorDescriptor();
        }
    };

    private static final long serialVersionUID = 1L;
    private ARecordType recType;
    private boolean[] openFields;

    public void reset(ARecordType recType, boolean[] openFields) {
        this.recType = recType;
        this.openFields = openFields;
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
                int n = args.length / 2;
                final IScalarEvaluator[] evalNames = new IScalarEvaluator[n];
                final IScalarEvaluator[] evalFields = new IScalarEvaluator[n];
                final IPointable fieldNamePointable = new VoidPointable();
                final IPointable fieldValuePointable = new VoidPointable();
                for (int i = 0; i < n; i++) {
                    evalNames[i] = args[2 * i].createScalarEvaluator(ctx);
                    evalFields[i] = args[2 * i + 1].createScalarEvaluator(ctx);
                }
                final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                final DataOutput out = resultStorage.getDataOutput();
                return new IScalarEvaluator() {
                    private RecordBuilder recBuilder = new RecordBuilder();
                    private int closedFieldId;
                    private boolean first = true;

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        try {
                            resultStorage.reset();
                            closedFieldId = 0;
                            if (first) {
                                first = false;
                                recBuilder.reset(recType);
                            }
                            recBuilder.init();
                            for (int i = 0; i < evalFields.length; i++) {
                                evalFields[i].evaluate(tuple, fieldValuePointable);
                                byte[] data = fieldValuePointable.getByteArray();
                                int offset = fieldValuePointable.getStartOffset();
                                boolean openField = openFields[i];
                                if (openField && data[offset] != ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                                    // MISSING for an open field means the field does not exist.
                                    evalNames[i].evaluate(tuple, fieldNamePointable);
                                    recBuilder.addField(fieldNamePointable, fieldValuePointable);
                                }
                                if (!openField && data[offset] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                                    // Turns MISSING into NULL for a closed field.
                                    data[offset] = ATypeTag.SERIALIZED_NULL_TYPE_TAG;
                                }
                                if (!openField) {
                                    recBuilder.addField(closedFieldId, fieldValuePointable);
                                    closedFieldId++;
                                }
                            }
                            recBuilder.write(out, true);
                            result.set(resultStorage);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }
}
