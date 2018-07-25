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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ArrayRepeatDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    AbstractCollectionType repeatedValueListType;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ArrayRepeatDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_ARGUMENTS_TYPE;
        }
    };

    @Override
    public void setImmutableStates(Object... states) {
        repeatedValueListType = new AOrderedListType((IAType) states[0], null);
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ARRAY_REPEAT;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws HyracksDataException {
                return new ArrayRepeatEval(args, ctx);
            }
        };
    }

    public class ArrayRepeatEval implements IScalarEvaluator {
        private final ArrayBackedValueStorage storage;
        private final IScalarEvaluator repeatedValueEval;
        private final IScalarEvaluator repeatEval;
        private final IPointable repeatedValueArg;
        private final IPointable repeatArg;
        private final TaggedValuePointable repeatArgValue;
        private final IAsterixListBuilder listBuilder;

        public ArrayRepeatEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            repeatedValueEval = args[0].createScalarEvaluator(ctx);
            repeatEval = args[1].createScalarEvaluator(ctx);
            repeatedValueArg = new VoidPointable();
            repeatArg = new VoidPointable();
            repeatArgValue = new TaggedValuePointable();
            listBuilder = new OrderedListBuilder();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            // 1st arg: value to repeat
            repeatedValueEval.evaluate(tuple, repeatedValueArg);

            // 2nd arg: number of repetitions
            repeatEval.evaluate(tuple, repeatArg);
            repeatArgValue.set(repeatArg);
            if (!ATypeHierarchy.isCompatible(ATypeTag.INTEGER, ATypeTag.VALUE_TYPE_MAPPING[repeatArgValue.getTag()])) {
                PointableHelper.setNull(result);
                return;
            }
            final String name = getIdentifier().getName();
            final int repetitions =
                    ATypeHierarchy.getIntegerValue(name, 1, repeatArg.getByteArray(), repeatArg.getStartOffset());

            // create list
            listBuilder.reset(repeatedValueListType);
            for (int i = 0; i < repetitions; ++i) {
                listBuilder.addItem(repeatedValueArg);
            }
            storage.reset();
            listBuilder.write(storage.getDataOutput(), true);
            result.set(storage);
        }
    }
}
