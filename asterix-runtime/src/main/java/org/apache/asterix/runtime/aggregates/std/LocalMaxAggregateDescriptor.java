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
package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import org.apache.hyracks.algebricks.runtime.base.ICopyAggregateFunctionFactory;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;

public class LocalMaxAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = AsterixBuiltinFunctions.LOCAL_MAX;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LocalMaxAggregateDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    public ICopyAggregateFunctionFactory createAggregateFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyAggregateFunctionFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyAggregateFunction createAggregateFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {
                return new MinMaxAggregateFunction(args, provider, false, true);
            }
        };
    }
}
