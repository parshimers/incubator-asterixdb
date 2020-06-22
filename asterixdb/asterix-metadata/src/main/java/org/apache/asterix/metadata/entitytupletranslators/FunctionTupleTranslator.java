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

package org.apache.asterix.metadata.entitytupletranslators;

import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_TYPE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FIELD_NAME_VALUE;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_NAME_FIELD_NAME;
import static org.apache.asterix.metadata.bootstrap.MetadataRecordTypes.PROPERTIES_VALUE_FIELD_NAME;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.BuiltinTypeMap;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.utils.TypeUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Function metadata entity to an ITupleReference and vice versa.
 */
public class FunctionTupleTranslator extends AbstractDatatypeTupleTranslator<Function> {

    // Payload field containing serialized Function.
    private static final int FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX = 3;

    protected OrderedListBuilder dependenciesListBuilder;
    protected OrderedListBuilder dependencyListBuilder;
    protected OrderedListBuilder dependencyNameListBuilder;
    protected List<String> dependencySubnames;
    protected AOrderedListType stringList;
    protected AOrderedListType listOfLists;

    protected FunctionTupleTranslator(TxnId txnId, MetadataNode metadataNode, boolean getTuple) {
        super(txnId, metadataNode, getTuple, MetadataPrimaryIndexes.FUNCTION_DATASET,
                FUNCTION_PAYLOAD_TUPLE_FIELD_INDEX);
        if (getTuple) {
            dependenciesListBuilder = new OrderedListBuilder();
            dependencyListBuilder = new OrderedListBuilder();
            dependencyNameListBuilder = new OrderedListBuilder();
            dependencySubnames = new ArrayList<>(3);
            stringList = new AOrderedListType(BuiltinType.ASTRING, null);
            listOfLists = new AOrderedListType(new AOrderedListType(BuiltinType.ASTRING, null), null);
        }
    }

    protected Function createMetadataEntityFromARecord(ARecord functionRecord) {
        String dataverseCanonicalName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX))
                        .getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String functionName =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX))
                        .getStringValue();
        int arity = Integer.parseInt(((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX)).getStringValue());

        IACursor paramNameCursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX)).getCursor();
        List<String> paramNames = new ArrayList<>();
        while (paramNameCursor.next()) {
            paramNames.add(((AString) paramNameCursor.get()).getStringValue());
        }

        List<TypeSignature> paramTypes = getParamTypes(functionRecord, arity, dataverseName);

        String returnTypeName = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX)).getStringValue();
        String returnTypeDataverseNameCanonical = getString(functionRecord, FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME);
        TypeSignature returnType = getTypeSignature(returnTypeName, returnTypeDataverseNameCanonical, dataverseName);

        String definition = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX)).getStringValue();
        String language = ((AString) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX)).getStringValue();
        String functionKind =
                ((AString) functionRecord.getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX))
                        .getStringValue();
        String functionLibrary = getString(functionRecord, FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME);
        Boolean nullCall = getBoolean(functionRecord, FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
        Boolean deterministic = getBoolean(functionRecord, FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);

        Map<String, String> resources = getResources(functionRecord);

        IACursor dependenciesCursor = ((AOrderedList) functionRecord
                .getValueByPos(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX)).getCursor();
        List<List<Triple<DataverseName, String, String>>> dependencies = new ArrayList<>();
        while (dependenciesCursor.next()) {
            List<Triple<DataverseName, String, String>> dependencyList = new ArrayList<>();
            IACursor qualifiedDependencyCursor = ((AOrderedList) dependenciesCursor.get()).getCursor();
            while (qualifiedDependencyCursor.next()) {
                Triple<DataverseName, String, String> dependency =
                        getDependency((AOrderedList) qualifiedDependencyCursor.get());
                dependencyList.add(dependency);
            }
            dependencies.add(dependencyList);
        }

        FunctionSignature signature = new FunctionSignature(dataverseName, functionName, arity);

        return new Function(signature, paramNames, paramTypes, returnType, definition, functionKind, language,
                functionLibrary, nullCall, deterministic, resources, dependencies);
    }

    private List<TypeSignature> getParamTypes(ARecord functionRecord, int arity, DataverseName functionDataverseName) {
        List<TypeSignature> paramTypes = new ArrayList<>(arity);
        ARecordType functionRecordType = functionRecord.getType();
        int paramTypesFieldIdx = functionRecordType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME);
        if (paramTypesFieldIdx >= 0) {
            IACursor cursor = ((AOrderedList) functionRecord.getValueByPos(paramTypesFieldIdx)).getCursor();
            while (cursor.next()) {
                ARecord paramTypeRecord = (ARecord) cursor.get();
                String paramTypeName = getString(paramTypeRecord, FIELD_NAME_TYPE);
                String paramTypeDataverseNameCanonical = getString(paramTypeRecord, FIELD_NAME_DATAVERSE_NAME);
                TypeSignature paramType =
                        getTypeSignature(paramTypeName, paramTypeDataverseNameCanonical, functionDataverseName);
                paramTypes.add(paramType);
            }
        } else {
            for (int i = 0; i < arity; i++) {
                paramTypes.add(TypeUtil.ANY_TYPE_SIGNATURE);
            }
        }
        return paramTypes;
    }

    private TypeSignature getTypeSignature(String typeName, String typeDataverseNameCanonical,
            DataverseName functionDataverseName) {
        if (BuiltinType.ANY.getTypeName().equals(typeName)) {
            return TypeUtil.ANY_TYPE_SIGNATURE;
        }
        BuiltinType builtinType = BuiltinTypeMap.getBuiltinType(typeName);
        if (builtinType != null) {
            return new TypeSignature(builtinType);
        }
        DataverseName typeDataverseName = typeDataverseNameCanonical == null ? functionDataverseName
                : DataverseName.createFromCanonicalForm(typeDataverseNameCanonical);
        return new TypeSignature(typeDataverseName, typeName);
    }

    private Triple<DataverseName, String, String> getDependency(AOrderedList dependencySubnames) {
        String dataverseCanonicalName = ((AString) dependencySubnames.getItem(0)).getStringValue();
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(dataverseCanonicalName);
        String second = null, third = null;
        int ln = dependencySubnames.size();
        if (ln > 1) {
            second = ((AString) dependencySubnames.getItem(1)).getStringValue();
            if (ln > 2) {
                third = ((AString) dependencySubnames.getItem(2)).getStringValue();
            }
        }
        return new Triple<>(dataverseName, second, third);
    }

    private Map<String, String> getResources(ARecord functionRecord) {
        Map<String, String> adaptorConfiguration = new HashMap<>();
        final ARecordType functionType = functionRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME);
        if (functionLibraryIdx >= 0) {
            IACursor cursor = ((AOrderedList) functionRecord.getValueByPos(functionLibraryIdx)).getCursor();
            while (cursor.next()) {
                ARecord field = (ARecord) cursor.get();
                final ARecordType fieldType = field.getType();
                final int keyIdx = fieldType.getFieldIndex(PROPERTIES_NAME_FIELD_NAME);
                String key = keyIdx >= 0 ? ((AString) field.getValueByPos(keyIdx)).getStringValue() : "";
                final int valueIdx = fieldType.getFieldIndex(PROPERTIES_VALUE_FIELD_NAME);
                String value = valueIdx >= 0 ? ((AString) field.getValueByPos(valueIdx)).getStringValue() : "";
                adaptorConfiguration.put(key, value);
            }
        }
        return adaptorConfiguration;
    }

    private String getString(ARecord aRecord, String fieldName) {
        final ARecordType functionType = aRecord.getType();
        final int functionLibraryIdx = functionType.getFieldIndex(fieldName);
        return functionLibraryIdx >= 0 ? ((AString) aRecord.getValueByPos(functionLibraryIdx)).getStringValue() : null;
    }

    private Boolean getBoolean(ARecord aRecord, String fieldName) {
        final ARecordType functionType = aRecord.getType();
        final int fieldIndex = functionType.getFieldIndex(fieldName);
        return fieldIndex >= 0 ? ((ABoolean) aRecord.getValueByPos(fieldIndex)).getBoolean() : null;
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Function function) throws HyracksDataException {
        DataverseName dataverseName = function.getDataverseName();
        String dataverseCanonicalName = dataverseName.getCanonicalForm();

        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(String.valueOf(function.getArity()));
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the fourth field of the tuple

        recordBuilder.reset(MetadataRecordTypes.FUNCTION_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(dataverseCanonicalName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(function.getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(String.valueOf(function.getArity()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX, fieldValue);

        // write field 3
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX]);
        for (String p : function.getParameterNames()) {
            itemValue.reset();
            aString.setValue(p);
            stringSerde.serialize(aString, itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX, fieldValue);

        // write field 4
        // Note: return type's dataverse name is written later in the open part
        fieldValue.reset();
        aString.setValue(function.getReturnType().getName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        aString.setValue(function.getFunctionBody());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(function.getLanguage());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        aString.setValue(function.getKind());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX, fieldValue);

        // write field 8
        dependenciesListBuilder.reset((AOrderedListType) MetadataRecordTypes.FUNCTION_RECORDTYPE
                .getFieldTypes()[MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX]);
        List<List<Triple<DataverseName, String, String>>> dependenciesList = function.getDependencies();
        for (List<Triple<DataverseName, String, String>> dependencies : dependenciesList) {
            dependencyListBuilder.reset(listOfLists);
            for (Triple<DataverseName, String, String> dependency : dependencies) {
                dependencyNameListBuilder.reset(stringList);
                for (String subName : getDependencySubNames(dependency)) {
                    itemValue.reset();
                    aString.setValue(subName);
                    stringSerde.serialize(aString, itemValue.getDataOutput());
                    dependencyNameListBuilder.addItem(itemValue);
                }
                itemValue.reset();
                dependencyNameListBuilder.write(itemValue.getDataOutput(), true);
                dependencyListBuilder.addItem(itemValue);

            }
            itemValue.reset();
            dependencyListBuilder.write(itemValue.getDataOutput(), true);
            dependenciesListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        dependenciesListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.FUNCTION_ARECORD_FUNCTION_DEPENDENCIES_FIELD_INDEX, fieldValue);

        writeOpenFields(function);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(Function function) throws HyracksDataException {
        writeReturnTypeDataverseName(function);
        writeParameterTypes(function);
        writeResources(function);
        writeLibrary(function);
        writeNullCall(function);
        writeDeterministic(function);
    }

    protected void writeResources(Function function) throws HyracksDataException {
        Map<String, String> withParams = function.getResources();
        if (withParams == null || withParams.isEmpty()) {
            return;
        }

        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (Map.Entry<String, String> property : withParams.entrySet()) {
            itemValue.reset();
            writePropertyTypeRecord(property.getKey(), property.getValue(), itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_WITHPARAM_LIST_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeParameterTypes(Function function) throws HyracksDataException {
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        listBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        for (TypeSignature paramType : function.getParameterTypes()) {
            itemValue.reset();
            writeTypeRecord(paramType.getDataverseName(), paramType.getName(), function.getDataverseName(),
                    itemValue.getDataOutput());
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);

        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_PARAMTYPES_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());

        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeLibrary(Function function) throws HyracksDataException {
        if (function.getLibrary() == null) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_LIBRARY_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(function.getLibrary());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeReturnTypeDataverseName(Function function) throws HyracksDataException {
        DataverseName returnTypeDataverseName = function.getReturnType().getDataverseName();
        boolean skipReturnTypeDataverseName =
                returnTypeDataverseName == null || returnTypeDataverseName.equals(function.getDataverseName());
        if (!skipReturnTypeDataverseName) {
            fieldName.reset();
            aString.setValue(FIELD_NAME_RETURN_TYPE_DATAVERSE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(returnTypeDataverseName.getCanonicalForm());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            recordBuilder.addField(fieldName, fieldValue);
        }
    }

    protected void writeNullCall(Function function) throws HyracksDataException {
        if (function.getNullCall() == null) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_NULLCALL_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        booleanSerde.serialize(ABoolean.valueOf(function.getNullCall()), fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    protected void writeDeterministic(Function function) throws HyracksDataException {
        if (function.getDeterministic() == null) {
            return;
        }
        fieldName.reset();
        aString.setValue(FUNCTION_ARECORD_FUNCTION_DETERMINISTIC_FIELD_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        booleanSerde.serialize(ABoolean.valueOf(function.getDeterministic()), fieldValue.getDataOutput());
        recordBuilder.addField(fieldName, fieldValue);
    }

    public void writePropertyTypeRecord(String name, String value, DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        // write field 0
        fieldName.reset();
        aString.setValue(FIELD_NAME_NAME);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field 1
        fieldName.reset();
        aString.setValue(FIELD_NAME_VALUE);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());

        propertyRecordBuilder.addField(fieldName, fieldValue);

        propertyRecordBuilder.write(out, true);
    }

    public void writeTypeRecord(DataverseName typeDataverseName, String typeName, DataverseName functionDataverseName,
            DataOutput out) throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        propertyRecordBuilder.reset(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);

        // write field "Type"
        fieldName.reset();
        aString.setValue(FIELD_NAME_TYPE);
        stringSerde.serialize(aString, fieldName.getDataOutput());
        fieldValue.reset();
        aString.setValue(typeName);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(fieldName, fieldValue);

        // write field "DataverseName"
        boolean skipTypeDataverseName = typeDataverseName == null || typeDataverseName.equals(functionDataverseName);
        if (!skipTypeDataverseName) {
            fieldName.reset();
            aString.setValue(FIELD_NAME_DATAVERSE_NAME);
            stringSerde.serialize(aString, fieldName.getDataOutput());
            fieldValue.reset();
            aString.setValue(typeDataverseName.getCanonicalForm());
            stringSerde.serialize(aString, fieldValue.getDataOutput());
            propertyRecordBuilder.addField(fieldName, fieldValue);
        }

        propertyRecordBuilder.write(out, true);
    }

    private List<String> getDependencySubNames(Triple<DataverseName, String, String> dependency) {
        dependencySubnames.clear();
        dependencySubnames.add(dependency.first.getCanonicalForm());
        if (dependency.second != null) {
            dependencySubnames.add(dependency.second);
        }
        if (dependency.third != null) {
            dependencySubnames.add(dependency.third);
        }
        return dependencySubnames;
    }
}