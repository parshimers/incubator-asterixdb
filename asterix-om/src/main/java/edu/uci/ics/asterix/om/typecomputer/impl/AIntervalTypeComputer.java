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
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class AIntervalTypeComputer implements IResultTypeComputer {

    public static final AIntervalTypeComputer INSTANCE = new AIntervalTypeComputer();

    private AIntervalTypeComputer() {
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer#computeType(edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression, edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment, edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider)
     */
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        return BuiltinType.AINTERVAL;
    }

}
