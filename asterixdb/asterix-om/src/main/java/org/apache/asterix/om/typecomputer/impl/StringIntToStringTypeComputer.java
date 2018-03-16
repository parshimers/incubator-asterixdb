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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class StringIntToStringTypeComputer extends AbstractResultTypeComputer {
    public static final StringIntToStringTypeComputer INSTANCE = new StringIntToStringTypeComputer(1);

    public static final StringIntToStringTypeComputer INSTANCE_TRIPLE_STRING = new StringIntToStringTypeComputer(3);

    private final int stringArgCount;

    public StringIntToStringTypeComputer(int stringArgCount) {
        this.stringArgCount = stringArgCount;
    }

    @Override
    public void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {
        ATypeTag tag = type.getTypeTag();
        if (argIndex < stringArgCount) {
            if (tag != ATypeTag.STRING) {
                throw new TypeMismatchException(funcName, argIndex, tag, ATypeTag.STRING);
            }
        } else {
            switch (tag) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    break;
                default:
                    throw new TypeMismatchException(funcName, argIndex, tag, ATypeTag.TINYINT, ATypeTag.SMALLINT,
                            ATypeTag.INTEGER, ATypeTag.BIGINT);
            }
        }
    }

    @Override
    public IAType getResultType(ILogicalExpression expr, IAType... types) throws AlgebricksException {
        return BuiltinType.ASTRING;
    }
}
