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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.library.java.base.JByte;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JFloat;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JShort;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.external.library.py.PyObjectPointableVisitor;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

public class PythonFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private final PointableAllocator pointableAllocator;
    private final PyObjectPointableVisitor pointableVisitor;
    private final Map<Integer, TypeInfo> poolTypeInfo;
    private final Map<String, String> parameters;
    private final IAType[] argTypes;
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ObjectNode args = new ObjectNode(JsonNodeFactory.instance);
    private final JSONDataParser jdp;
    private final String packageName;
    private final URL[] libraryPaths;
    private final ClassLoader cl;
    private final Object[] arguments;
    public static final BiMap<Class, Class> typeConv =
            new ImmutableBiMap.Builder<Class, Class>().put(HashMap.class, JRecord.class).put(Byte.class, JByte.class)
                    .put(Short.class, JShort.class).put(Integer.class, JInt.class).put(Long.class, JLong.class)
                    .put(Float.class, JFloat.class).put(Double.class, JDouble.class)
                    .put(ArrayList.class, JOrderedList.class).put(String.class, JString.class).build();

    private boolean isValidResult = false;

    public PythonFunctionHelper(IExternalFunctionInfo finfo, IAType[] argTypes, IDataOutputProvider outputProvider,
            URL[] libraryPaths, ClassLoader cl) {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        this.pointableAllocator = new PointableAllocator();
        this.arguments = new Object[finfo.getArgumentList().size()];
        this.poolTypeInfo = new HashMap<>();
        this.parameters = finfo.getParams();
        pointableVisitor = new PyObjectPointableVisitor();
        this.argTypes = argTypes;
        this.jdp = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, new JsonFactory());
        this.packageName = finfo.getFunctionBody().trim();
        this.libraryPaths = libraryPaths;
        this.cl = cl;
    }

    public Object[] getArguments() {
        return arguments;
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

    public void setResult(Object o) throws IllegalAccessException, InstantiationException, HyracksDataException {
        Class concrete = o.getClass();
        Class asxConv = typeConv.get(concrete);
        IJObject serde = (IJObject) asxConv.newInstance();
        serde.setValue(o);
        serde.serialize(outputProvider.getDataOutput(), true);
        isValidResult = true;
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

    public Object getArgumentPrim(int index) {
        return arguments[index];
    }

    public void setArgument(int index, IValueReference valueReference) throws IOException, AsterixException {
        IVisitablePointable pointable;
        Object obj = null;
        IAType type = argTypes[index];
        switch (type.getTypeTag()) {
            case OBJECT:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((ARecordVisitablePointable) pointable, type);
                break;
            case ARRAY:
            case MULTISET:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((AListVisitablePointable) pointable, type);
                break;
            case ANY:
                TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
                pointy.set(valueReference);
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                switch (rtTypeTag) {
                    case OBJECT:
                        pointable = pointableAllocator.allocateRecordValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((ARecordVisitablePointable) pointable, rtType);
                        break;
                    case ARRAY:
                    case MULTISET:
                        pointable = pointableAllocator.allocateListValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((AListVisitablePointable) pointable, rtType);
                        break;
                    default:
                        pointable = pointableAllocator.allocateFieldValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((AFlatValuePointable) pointable, rtType);
                        break;
                }
                break;
            default:
                pointable = pointableAllocator.allocateFieldValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((AFlatValuePointable) pointable, type);
                break;
        }
        arguments[index] = obj;
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

    @Override
    public IJObject getResultObject(IAType type) {
        return null;
    }

    @Override
    public IJObject getObject(JTypeTag jtypeTag) throws RuntimeDataException {
        return null;
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
