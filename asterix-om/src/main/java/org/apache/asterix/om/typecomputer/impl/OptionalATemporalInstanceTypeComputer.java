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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class OptionalATemporalInstanceTypeComputer implements IResultTypeComputer {

    public static final OptionalATemporalInstanceTypeComputer INSTANCE = new OptionalATemporalInstanceTypeComputer();

    private OptionalATemporalInstanceTypeComputer() {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer#computeType(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression, edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment, edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider)
     */
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        unionList.add(BuiltinType.ADATE);
        unionList.add(BuiltinType.ATIME);
        unionList.add(BuiltinType.ADATETIME);
        return new AUnionType(unionList, "OptionalTemporalInstance");
    }

}
