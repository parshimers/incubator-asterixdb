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
package org.apache.asterix.external.library;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JNull;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.fasterxml.jackson.core.JsonFactory;

public class PythonFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private final PointableAllocator pointableAllocator;
    private final Map<Integer, TypeInfo> poolTypeInfo;
    private final Map<String, String> parameters;
    private final IAType[] argTypes;
    private final String[] stringArgs;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ObjectNode args = new ObjectNode(JsonNodeFactory.instance);
    private final JSONDataParser jdp;
    private final String packageName;
    private final IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<>(JTypeObjectFactory.INSTANCE);
    private final URL[] libraryPaths;
    private final ClassLoader cl;

    private boolean isValidResult = false;

    public PythonFunctionHelper(IExternalFunctionInfo finfo, IAType[] argTypes, IDataOutputProvider outputProvider,
            URL[] libraryPaths, ClassLoader cl) {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        this.pointableAllocator = new PointableAllocator();
        this.stringArgs = new String[finfo.getArgumentList().size()];
        this.poolTypeInfo = new HashMap<>();
        this.parameters = finfo.getParams();
        this.argTypes = argTypes;
        this.jdp = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, new JsonFactory());
        this.packageName = finfo.getFunctionBody().trim();
        this.libraryPaths = libraryPaths;
        this.cl = cl;
    }

    @Override
    public String[] getArgumentStr() {
        return stringArgs;
    }

    @Override
    public String getArgumentsJSON() {
        return args.toString();
    }

    @Override
    public void setResult(IJObject result) throws HyracksDataException {
        if (result == null || checkInvalidReturnValueType(result, finfo.getReturnType())) {
            isValidResult = false;
        } else {
            isValidResult = true;
            result.serialize(outputProvider.getDataOutput(), true);
            result.reset();
        }
    }

    @Override
    public void setResultJSON(String json) throws HyracksDataException {
        try {
            jdp.setInputStream(org.apache.commons.io.IOUtils.toInputStream(json));
            jdp.parse(outputProvider.getDataOutput());
            isValidResult = true;
        } catch (IOException e) {
            throw new HyracksDataException(e.toString());
        }

    }

    private boolean checkInvalidReturnValueType(IJObject result, IAType expectedType) {
        if (expectedType.equals(BuiltinType.ANY)) {
            return false;
        }
        if (!expectedType.deepEqual(result.getIAType())) {
            return true;
        }
        return false;
    }

    /**
     * Gets the value of the result flag
     *
     * @return
     *         boolean True is the setResult is called and result is not null
     */
    @Override
    public boolean isValidResult() {
        return this.isValidResult;
    }

    @Override
    public IJObject getArgument(int index) {
        return null;
    }

    public void setArgument(int index, IValueReference valueReference) throws HyracksDataException {
        APrintVisitor pv = new APrintVisitor();
        IVisitablePointable pointable = null;
        IJObject jObject = null;
        IAType type = argTypes[index];
        out.reset();
        PrintStream ps = new PrintStream(out);
        Pair<PrintStream, ATypeTag> pst = new Pair(ps, getTypeInfo(index, type).getTypeTag());
        switch (type.getTypeTag()) {
            case OBJECT:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                pv.visit((ARecordVisitablePointable) pointable, pst);
                break;
            case ARRAY:
            case MULTISET:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                pv.visit((AListVisitablePointable) pointable, pst);
                break;
            case ANY:
                TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
                pointy.set(valueReference);
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                pst = new Pair(ps, rtTypeTag);
                switch (rtTypeTag) {
                    case OBJECT:
                        pointable = pointableAllocator.allocateRecordValue(rtType);
                        pointable.set(valueReference);
                        pv.visit((ARecordVisitablePointable) pointable, pst);
                        break;
                    case ARRAY:
                    case MULTISET:
                        pointable = pointableAllocator.allocateListValue(rtType);
                        pointable.set(valueReference);
                        pv.visit((AListVisitablePointable) pointable, pst);
                        break;
                    default:
                        pointable = pointableAllocator.allocateFieldValue(rtType);
                        pointable.set(valueReference);
                        pv.visit((AFlatValuePointable) pointable, pst);
                        break;
                }
                break;
            default:
                pointable = pointableAllocator.allocateFieldValue(type);
                pointable.set(valueReference);
                pv.visit((AFlatValuePointable) pointable, pst);
                break;
        }
        ps.flush();
        stringArgs[index] = out.toString();
        args.put(Integer.valueOf(index).toString(), stringArgs[index]);
    }

    public URL[] getLibraryDeployedPath() {
        return libraryPaths;
    }

    public ClassLoader getClassLoader() {
        return cl;
    }

    @Override
    public IJObject getResultObject() {
        return null;
    }

    private TypeInfo getTypeInfo(int index, IAType type) {
        TypeInfo typeInfo = poolTypeInfo.get(index);
        if (typeInfo == null) {
            typeInfo = new TypeInfo(objectPool, type, type.getTypeTag());
            poolTypeInfo.put(index, typeInfo);
        }
        return typeInfo;
    }

    @Override
    public IJObject getResultObject(IAType type) {
        return null;
    }

    @Override
    public IJObject getObject(JTypeTag jtypeTag) throws RuntimeDataException {
        IJObject retValue = null;
        switch (jtypeTag) {
            case INT:
                retValue = objectPool.allocate(BuiltinType.AINT32);
                break;
            case STRING:
                retValue = objectPool.allocate(BuiltinType.ASTRING);
                break;
            case DOUBLE:
                retValue = objectPool.allocate(BuiltinType.ADOUBLE);
                break;
            case NULL:
                retValue = JNull.INSTANCE;
                break;
            default:
                try {
                    throw new RuntimeDataException(ErrorCode.LIBRARY_JAVA_FUNCTION_HELPER_OBJ_TYPE_NOT_SUPPORTED,
                            jtypeTag.name());
                } catch (IllegalStateException e) {
                    // Exception is not thrown
                    e.printStackTrace();
                }
                break;
        }
        return retValue;
    }

    @Override
    public void reset() {
        pointableAllocator.reset();
    }

    @Override
    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public String getExternalIdentifier() {
        return packageName;
    }

}
