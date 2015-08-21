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
package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedNumericAggTypeComputer implements IResultTypeComputer {

    private static final String errMsg = "Aggregator is not implemented for ";

    public static final NonTaggedNumericAggTypeComputer INSTANCE = new NonTaggedNumericAggTypeComputer();

    private NonTaggedNumericAggTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        IAType t1 = (IAType) env.getType(arg1);
        if (t1 == null) {
            return null;
        }

        ATypeTag tag1;
        if (NonTaggedFormatUtil.isOptional(t1)) {
            tag1 = ((AUnionType) t1).getNullableType().getTypeTag();
        } else {
            tag1 = t1.getTypeTag();
        }

        IAType type;
        switch (tag1) {
            case DOUBLE:
                type = BuiltinType.ADOUBLE;
                break;
            case FLOAT:
                type = BuiltinType.AFLOAT;
                break;
            case INT64:
                type = BuiltinType.AINT64;
                break;
            case INT32:
                type = BuiltinType.AINT32;
                break;
            case INT16:
                type = BuiltinType.AINT16;
                break;
            case INT8:
                type = BuiltinType.AINT8;
                break;
            case ANY:
                return BuiltinType.ANY;
            default: {
                throw new NotImplementedException(errMsg + tag1);
            }
        }
        return AUnionType.createNullableType(type, "SumResult");
    }
}
