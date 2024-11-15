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

package edu.uci.ics.asterix.metadata.entities;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.ExternalDatasetTransactionState;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.om.base.ADateTime;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class ExternalDatasetDetails implements IDatasetDetails {

    private static final long serialVersionUID = 1L;
    private final String adapter;
    private final Map<String, String> properties;
    private final String nodeGroupName;
    private final long addToCacheTime;
    private Date lastRefreshTime;
    private ExternalDatasetTransactionState state;
    protected String compactionPolicy;
    protected Map<String, String> compactionPolicyProperties;

    public ExternalDatasetDetails(String adapter, Map<String, String> properties, String nodeGroupName,
            Date lastRefreshTime, ExternalDatasetTransactionState state, String compactionPolicy,
            Map<String, String> compactionPolicyProperties) {
        this.properties = properties;
        this.adapter = adapter;
        this.nodeGroupName = nodeGroupName;
        this.addToCacheTime = System.currentTimeMillis();
        this.lastRefreshTime = lastRefreshTime;
        this.state = state;
        this.compactionPolicy = compactionPolicy;
        this.compactionPolicyProperties = compactionPolicyProperties;
    }

    public String getAdapter() {
        return adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public DatasetType getDatasetType() {
        return DatasetType.EXTERNAL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeDatasetDetailsRecordType(DataOutput out) throws HyracksDataException {
        IARecordBuilder externalRecordBuilder = new RecordBuilder();
        OrderedListBuilder listBuilder = new OrderedListBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
        externalRecordBuilder.reset(MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE);
        AMutableString aString = new AMutableString("");

        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);
        ISerializerDeserializer<ADateTime> dateTimeSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ADATETIME);
        ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT32);

        // write field 0
        fieldValue.reset();
        aString.setValue(this.getAdapter());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX,
                fieldValue);

        // write field 1
        listBuilder.reset((AOrderedListType) MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE.getFieldTypes()[1]);
        for (Map.Entry<String, String> property : this.properties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                    MetadataRecordTypes.DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(getNodeGroupName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_GROUPNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        dateTimeSerde.serialize(new ADateTime(lastRefreshTime.getTime()), fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX,
                fieldValue);

        // write field 4
        fieldValue.reset();
        intSerde.serialize(new AInt32(state.ordinal()), fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX,
                fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(getCompactionPolicy().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        externalRecordBuilder.addField(MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_COMPACTION_POLICY_FIELD_INDEX,
                fieldValue);

        // write field 7
        listBuilder
                .reset((AOrderedListType) MetadataRecordTypes.EXTERNAL_DETAILS_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX]);
        for (Map.Entry<String, String> property : compactionPolicyProperties.entrySet()) {
            String name = property.getKey();
            String value = property.getValue();
            itemValue.reset();
            writePropertyTypeRecord(name, value, itemValue.getDataOutput(),
                    MetadataRecordTypes.COMPACTION_POLICY_PROPERTIES_RECORDTYPE);
            listBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        listBuilder.write(fieldValue.getDataOutput(), true);
        externalRecordBuilder.addField(
                MetadataRecordTypes.EXTERNAL_DETAILS_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX, fieldValue);
        try {
            externalRecordBuilder.write(out, true);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }

    }

    @SuppressWarnings("unchecked")
    protected void writePropertyTypeRecord(String name, String value, DataOutput out, ARecordType recordType)
            throws HyracksDataException {
        IARecordBuilder propertyRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        propertyRecordBuilder.reset(recordType);
        AMutableString aString = new AMutableString("");
        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ASTRING);

        // write field 0
        fieldValue.reset();
        aString.setValue(name);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(0, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(value);
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        propertyRecordBuilder.addField(1, fieldValue);

        try {
            propertyRecordBuilder.write(out, true);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public String getNodeGroupName() {
        return nodeGroupName;
    }

    @Override
    public boolean isTemp() {
        return false;
    }

    @Override
    public long getLastAccessTime() {
        return addToCacheTime;
    }

    public Date getTimestamp() {
        return lastRefreshTime;
    }

    public void setRefreshTimestamp(Date timestamp) {
        this.lastRefreshTime = timestamp;
    }

    public ExternalDatasetTransactionState getState() {
        return state;
    }

    public void setState(ExternalDatasetTransactionState state) {
        this.state = state;
    }

    @Override
    public String getCompactionPolicy() {
        return compactionPolicy;
    }

    @Override
    public Map<String, String> getCompactionPolicyProperties() {
        return compactionPolicyProperties;
    }
}