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

import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.exceptions.UnsupportedTypeException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class NumericDivideDescriptor extends AbstractNumericArithmeticEval {
    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new NumericDivideDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.NUMERIC_DIVIDE;
    }

    @Override
    protected ATypeTag getNumericResultType(ATypeTag argTypeMax) {
        return argTypeMax.ordinal() < ATypeTag.FLOAT.ordinal() ? ATypeTag.DOUBLE : argTypeMax;
    }

    @Override
    protected boolean evaluateDouble(double lhs, double rhs, AMutableDouble result) {
        if (rhs == 0) {
            return false; // result = NULL
        }
        double res = lhs / rhs;
        result.setValue(res);
        return true;
    }

    @Override
    protected boolean evaluateInteger(long lhs, long rhs, AMutableInt64 result) {
        throw new IllegalStateException();
    }

    @Override
    protected boolean evaluateTimeDurationArithmetic(long chronon, int yearMonth, long dayTime, boolean isTimeOnly,
            AMutableInt64 result) {
        throw new NotImplementedException("Divide operation is not defined for temporal types");
    }

    @Override
    protected boolean evaluateTimeInstanceArithmetic(long chronon0, long chronon1, AMutableInt64 result)
            throws HyracksDataException {
        throw new UnsupportedTypeException(sourceLoc, getIdentifier(), ATypeTag.SERIALIZED_TIME_TYPE_TAG);
    }
}
