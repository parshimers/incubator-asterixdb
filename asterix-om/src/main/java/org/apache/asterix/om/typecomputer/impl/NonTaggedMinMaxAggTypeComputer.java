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
package org.apache.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedMinMaxAggTypeComputer implements IResultTypeComputer {
    private static final String errMsg = "Aggregator is not implemented for ";

    public static final NonTaggedMinMaxAggTypeComputer INSTANCE = new NonTaggedMinMaxAggTypeComputer();

    private NonTaggedMinMaxAggTypeComputer() {
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
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1)) {
            tag1 = ((AUnionType) t1).getUnionList().get(AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        } else {
            tag1 = t1.getTypeTag();
        }

        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);

        switch (tag1) {
            case DOUBLE:
                unionList.add(BuiltinType.ADOUBLE);
                break;
            case FLOAT:
                unionList.add(BuiltinType.AFLOAT);
                break;
            case INT64:
                unionList.add(BuiltinType.AINT64);
                break;
            case INT32:
                unionList.add(BuiltinType.AINT32);
                break;
            case INT16:
                unionList.add(BuiltinType.AINT16);
                break;
            case INT8:
                unionList.add(BuiltinType.AINT8);
                break;
            case STRING:
                unionList.add(BuiltinType.ASTRING);
                break;
            case DATE:
                unionList.add(BuiltinType.ADATE);
                break;
            case TIME:
                unionList.add(BuiltinType.ATIME);
                break;
            case DATETIME:
                unionList.add(BuiltinType.ADATETIME);
                break;
            case YEARMONTHDURATION:
                unionList.add(BuiltinType.AYEARMONTHDURATION);
                break;
            case DAYTIMEDURATION:
                unionList.add(BuiltinType.ADAYTIMEDURATION);
                break;
            case ANY:
                return BuiltinType.ANY;
            default: {
                throw new NotImplementedException(errMsg + tag1);
            }
        }
        return new AUnionType(unionList, "SumResult");
    }
}
