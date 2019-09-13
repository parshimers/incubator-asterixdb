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
package org.apache.asterix.metadata.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class ExternalFunctionCompilerUtil {

    private static Pattern orderedListPattern = Pattern.compile("\\[*\\]");
    private static Pattern unorderedListPattern = Pattern.compile("[{{*}}]");

    private ExternalFunctionCompilerUtil() {
        // do nothing
    }

    public static IFunctionInfo getExternalFunctionInfo(MetadataTransactionContext txnCtx, Function function)
            throws AlgebricksException {

        String functionKind = function.getKind();
        IFunctionInfo finfo = null;
        if (FunctionKind.SCALAR.toString().equalsIgnoreCase(functionKind)) {
            finfo = getScalarFunctionInfo(txnCtx, function);
        } else if (FunctionKind.AGGREGATE.toString().equalsIgnoreCase(functionKind)) {
            finfo = getAggregateFunctionInfo(txnCtx, function);
        } else if (FunctionKind.STATEFUL.toString().equalsIgnoreCase(functionKind)) {
            finfo = getStatefulFunctionInfo(txnCtx, function);
        } else if (FunctionKind.UNNEST.toString().equalsIgnoreCase(functionKind)) {
            finfo = getUnnestFunctionInfo(txnCtx, function);
        }
        return finfo;
    }

    private static IFunctionInfo getScalarFunctionInfo(MetadataTransactionContext txnCtx, Function function)
            throws AlgebricksException {
        FunctionIdentifier fid =
                new FunctionIdentifier(function.getDataverseName(), function.getName(), function.getArity());
        List<IAType> argumentTypes = new ArrayList<>();
        IAType returnType;
        if (function.getReturnType() == null) {
            returnType = BuiltinType.ANY;
        } else if (function.getReturnType().equalsIgnoreCase(BuiltinType.ANY.toString())) {
            returnType = BuiltinType.ANY;
        } else {
            returnType = BuiltinTypeMap.getTypeFromTypeName(MetadataNode.INSTANCE, txnCtx.getTxnId(),
                    function.getDataverseName(), function.getReturnType().split("\\.")[1], false);
        }
        for (String arg : function.getArguments()) {
            if (arg == null) {
                argumentTypes.add(null);
            } else {
                argumentTypes.add(BuiltinTypeMap.getTypeFromTypeName(MetadataNode.INSTANCE, txnCtx.getTxnId(),
                        function.getDataverseName(), arg, false));
            }
        }
        IResultTypeComputer typeComputer = new ExternalTypeComputer(returnType, argumentTypes);

        return new ExternalScalarFunctionInfo(fid.getNamespace(), function.getLibrary(), fid.getName(), fid.getArity(),
                returnType, function.getFunctionBody(), function.getLanguage(), argumentTypes, typeComputer);
    }

    private static IFunctionInfo getUnnestFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

    private static IFunctionInfo getStatefulFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

    private static IFunctionInfo getAggregateFunctionInfo(MetadataTransactionContext txnCtx, Function function) {
        return null;
    }

}
