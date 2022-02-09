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
package org.apache.asterix.external.library;

import org.apache.asterix.om.functions.*;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.aggregates.scalar.AbstractScalarAggregateDescriptor;
import org.apache.asterix.runtime.unnestingfunctions.std.ScanCollectionDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.ColumnAccessEvalFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ScalarExternalAggregateDescriptor extends AbstractScalarAggregateDescriptor
        implements IExternalFunctionDescriptor {

    private static final long serialVersionUID = 1L;
    ExternalAggregateFunctionDescriptorFactory extFact;
    private IAType[] argTypes;

    public ScalarExternalAggregateDescriptor(IFunctionDescriptorFactory factory) {
        super(factory);
        extFact = (ExternalAggregateFunctionDescriptorFactory) factory;
    }

    @Override
    public void setImmutableStates(Object... states) {
        argTypes = new IAType[states.length];
        for (int i = 0; i < states.length; i++) {
            argTypes[i] = (IAType) states[i];
        }
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {

        // The aggregate function will get a SingleFieldFrameTupleReference that points to the result of the
        // ScanCollection. The list-item will always reside in the first field (column) of the
        // SingleFieldFrameTupleReference.
        int numArgs = args.length;
        IScalarEvaluatorFactory[] aggFuncArgs = new IScalarEvaluatorFactory[numArgs];

        aggFuncArgs[0] = new ColumnAccessEvalFactory(0);

        for (int i = 1; i < numArgs; ++i) {
            aggFuncArgs[i] = args[i];
        }

        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                // Use ScanCollection to iterate over list items.
                ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory scanCollectionFactory =
                        new ScanCollectionDescriptor.ScanCollectionUnnestingFunctionFactory(args[0], sourceLoc);
                IExternalFunctionInfo finfo = ((ExternalAggregateFunctionDescriptor) aggFuncDesc).getFunctionInfo();
                AggregateExternalTypeComputer typeComputer = new AggregateExternalTypeComputer(finfo.getReturnType(),
                        finfo.getParameterTypes(), finfo.getNullCall());
                ExternalFunctionInfo externalFunctionInfo = new ExternalFunctionInfo(finfo.getFunctionIdentifier(),
                        finfo.getKind(), finfo.getParameterTypes(), finfo.getReturnType(), typeComputer,
                        finfo.getLanguage(), finfo.getLibraryDataverseName(), finfo.getLibraryName(),
                        finfo.getExternalIdentifier(), finfo.getResources(), finfo.isFunctional(), finfo.getNullCall());
                IAggregateEvaluatorFactory aggregateFunctionEvaluatorFactory = new ExternalAggregateFunctionEvaluatorFactory(externalFunctionInfo, aggFuncArgs, argTypes, sourceLoc);

                return createScalarAggregateEvaluator(aggregateFunctionEvaluatorFactory.createAggregateEvaluator(ctx),
                        scanCollectionFactory, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return extFact.getfInfo().getFunctionIdentifier();
    }

    @Override
    public IExternalFunctionInfo getFunctionInfo() {
        return extFact.getfInfo();
    }

    @Override
    public IAType[] getArgumentTypes() {
        return argTypes;
    }
}
