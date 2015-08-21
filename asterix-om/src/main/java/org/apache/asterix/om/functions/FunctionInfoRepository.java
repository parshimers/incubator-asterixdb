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
package org.apache.asterix.om.functions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionInfoRepository {
    private final Map<FunctionSignature, IFunctionInfo> functionMap;

    public FunctionInfoRepository() {
        functionMap = new ConcurrentHashMap<FunctionSignature, IFunctionInfo>();
    }

    public IFunctionInfo get(String namespace, String name, int arity) {
        FunctionSignature fname = new FunctionSignature(namespace, name, arity);
        return functionMap.get(fname);
    }

    public IFunctionInfo get(FunctionIdentifier fid) {
        return get(fid.getNamespace(), fid.getName(), fid.getArity());
    }

    public void put(FunctionIdentifier fid, IFunctionInfo fInfo) {
        FunctionSignature functionSignature = new FunctionSignature(fid.getNamespace(), fid.getName(), fid.getArity());
        functionMap.put(functionSignature, fInfo);
    }
}
