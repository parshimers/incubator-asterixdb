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

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.typecomputer.base.TypeComputerUtilities;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class OrderedListConstructorResultType implements IResultTypeComputer {

    public static final OrderedListConstructorResultType INSTANCE = new OrderedListConstructorResultType();

    @Override
    public AOrderedListType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        /**
         * if type has been top-down propagated, use the enforced type
         */
        AOrderedListType reqType = (AOrderedListType) TypeComputerUtilities.getRequiredType(f);
        if (reqType != null)
            return reqType;

        ArrayList<IAType> types = new ArrayList<IAType>();
        for (int k = 0; k < f.getArguments().size(); k++) {
            IAType type = (IAType) env.getType(f.getArguments().get(k).getValue());
            if (type.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) type))
                type = ((AUnionType) type).getUnionList().get(AUnionType.OPTIONAL_TYPE_INDEX_IN_UNION_LIST);
            if (types.indexOf(type) < 0) {
                types.add(type);
            }
        }
        if (types.size() == 1) {
            return new AOrderedListType(types.get(0), null);
        } else {
            return new AOrderedListType(BuiltinType.ANY, null);
        }

    }
}
