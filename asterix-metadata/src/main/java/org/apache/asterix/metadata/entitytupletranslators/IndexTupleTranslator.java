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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.config.DatasetConfig.IndexTypeProperty;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.metadata.MetadataNode;
import org.apache.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.AsterixBuiltinTypeMap;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.ACollectionCursor;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates an Index metadata entity to an ITupleReference and vice versa.
 */
public class IndexTupleTranslator extends AbstractTupleTranslator<Index> {
    // Field indexes of serialized Index in a tuple.
    // First key field.
    public static final int INDEX_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // Second key field.
    public static final int INDEX_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Third key field.
    public static final int INDEX_INDEXNAME_TUPLE_FIELD_INDEX = 2;
    // Payload field containing serialized Index.
    public static final int INDEX_PAYLOAD_TUPLE_FIELD_INDEX = 3;
    // Field names of open fields.
    public static final String GRAM_LENGTH_FIELD_NAME = "GramLength";
    public static final String BOTTOM_LEFT_X_FIELD_NAME = "BottomLeftX";
    public static final String BOTTOM_LEFT_Y_FIELD_NAME = "BottomLeftY";
    public static final String TOP_RIGHT_X_FIELD_NAME = "TopRightX";
    public static final String TOP_RIGHT_Y_FIELD_NAME = "TopRightY";
    public static final String LEVEL_DENSITY_FIELD_NAME_PREFIX = "LevelDensity";
    public static final String CELLS_PER_OBJECT_FIELD_NAME = "CellsPerObject";
    public static final String INDEX_SEARCHKEY_TYPE_FIELD_NAME = "SearchKeyType";
    public static final String INDEX_ISENFORCED_FIELD_NAME = "IsEnforced";

    private OrderedListBuilder listBuilder = new OrderedListBuilder();
    private OrderedListBuilder primaryKeyListBuilder = new OrderedListBuilder();
    private AOrderedListType stringList = new AOrderedListType(BuiltinType.ASTRING, null);
    private ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
    private List<List<String>> searchKey;
    private List<IAType> searchKeyType;
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt32> intSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.INDEX_RECORDTYPE);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt64> longSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<AInt16> shortSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT16);
    private final MetadataNode metadataNode;
    private final JobId jobId;

    public IndexTupleTranslator(JobId jobId, MetadataNode metadataNode, boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.INDEX_DATASET.getFieldCount());
        this.jobId = jobId;
        this.metadataNode = metadataNode;
    }

    @Override
    public Index getMetadataEntityFromTuple(ITupleReference frameTuple) throws IOException, MetadataException {
        byte[] serRecord = frameTuple.getFieldData(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = frameTuple.getFieldStart(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = frameTuple.getFieldLength(INDEX_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord rec = (ARecord) recordSerde.deserialize(in);
        String dvName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX))
                .getStringValue();
        String dsName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX))
                .getStringValue();
        String indexName = ((AString) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX))
                .getStringValue();
        IndexType indexStructure = IndexType.valueOf(((AString) rec
                .getValueByPos(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX)).getStringValue());
        IACursor fieldNameCursor = ((AOrderedList) rec
                .getValueByPos(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX)).getCursor();
        List<List<String>> searchKey = new ArrayList<List<String>>();
        AOrderedList fieldNameList;
        while (fieldNameCursor.next()) {
            fieldNameList = (AOrderedList) fieldNameCursor.get();
            IACursor nestedFieldNameCursor = (fieldNameList.getCursor());
            List<String> nestedFieldName = new ArrayList<String>();
            while (nestedFieldNameCursor.next()) {
                nestedFieldName.add(((AString) nestedFieldNameCursor.get()).getStringValue());
            }
            searchKey.add(nestedFieldName);
        }
        int indexKeyTypeFieldPos = rec.getType().findFieldPosition(INDEX_SEARCHKEY_TYPE_FIELD_NAME);
        IACursor fieldTypeCursor = new ACollectionCursor();
        if (indexKeyTypeFieldPos > 0)
            fieldTypeCursor = ((AOrderedList) rec.getValueByPos(indexKeyTypeFieldPos)).getCursor();
        List<IAType> searchKeyType = new ArrayList<IAType>(searchKey.size());
        while (fieldTypeCursor.next()) {
            String typeName = ((AString) fieldTypeCursor.get()).getStringValue();
            IAType fieldType = AsterixBuiltinTypeMap.getTypeFromTypeName(metadataNode, jobId, dvName, typeName, false);
            searchKeyType.add(fieldType);
        }
        // index key type information is not persisted, thus we extract type information from the record metadata
        if (searchKeyType.isEmpty()) {
            String datatypeName = metadataNode.getDataset(jobId, dvName, dsName).getItemTypeName();
            ARecordType recordDt = (ARecordType) metadataNode.getDatatype(jobId, dvName, datatypeName).getDatatype();
            for (int i = 0; i < searchKey.size(); i++) {
                IAType fieldType = recordDt.getSubFieldType(searchKey.get(i));
                searchKeyType.add(fieldType);
            }
        }
        int isEnforcedFieldPos = rec.getType().findFieldPosition(INDEX_ISENFORCED_FIELD_NAME);
        Boolean isEnforcingKeys = false;
        if (isEnforcedFieldPos > 0)
            isEnforcingKeys = ((ABoolean) rec.getValueByPos(isEnforcedFieldPos)).getBoolean();
        Boolean isPrimaryIndex = ((ABoolean) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX))
                .getBoolean();
        int pendingOp = ((AInt32) rec.getValueByPos(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX))
                .getIntegerValue();
        IndexTypeProperty indexTypeProperty = new IndexTypeProperty();
        // Check if there is a gram length as well.
        indexTypeProperty.gramLength = -1;
        int fieldPos = rec.getType().findFieldPosition(GRAM_LENGTH_FIELD_NAME);
        if (fieldPos >= 0) {
            indexTypeProperty.gramLength = ((AInt32) rec.getValueByPos(fieldPos)).getIntegerValue();
        }

        // read optional fields for spatial index types
        fieldPos = rec.getType().findFieldPosition(BOTTOM_LEFT_X_FIELD_NAME);
        if (fieldPos >= 0) {
            indexTypeProperty.bottomLeftX = ((ADouble) rec.getValueByPos(fieldPos)).getDoubleValue();
            indexTypeProperty.bottomLeftY = ((ADouble) rec.getValueByPos(rec.getType().findFieldPosition(
                    BOTTOM_LEFT_Y_FIELD_NAME))).getDoubleValue();
            indexTypeProperty.topRightX = ((ADouble) rec.getValueByPos(rec.getType().findFieldPosition(
                    TOP_RIGHT_X_FIELD_NAME))).getDoubleValue();
            indexTypeProperty.topRightY = ((ADouble) rec.getValueByPos(rec.getType().findFieldPosition(
                    TOP_RIGHT_Y_FIELD_NAME))).getDoubleValue();
            for (int i = 0; i < IndexTypeProperty.CELL_BASED_SPATIAL_INDEX_MAX_LEVEL; i++) {
                indexTypeProperty.levelDensity[i] = ((AInt16) rec.getValueByPos(rec.getType().findFieldPosition(
                        LEVEL_DENSITY_FIELD_NAME_PREFIX + i))).getShortValue();
            }
            indexTypeProperty.cellsPerObject = ((AInt32) rec.getValueByPos(rec.getType().findFieldPosition(
                    CELLS_PER_OBJECT_FIELD_NAME))).getIntegerValue();
        }
        return new Index(dvName, dsName, indexName, indexStructure, indexTypeProperty, searchKey, searchKeyType,
                isEnforcingKeys, isPrimaryIndex, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Index instance) throws IOException, MetadataException {
        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(instance.getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(instance.getIndexName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the fourth field of the tuple
        recordBuilder.reset(MetadataRecordTypes.INDEX_RECORDTYPE);
        // write field 0
        fieldValue.reset();
        aString.setValue(instance.getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(instance.getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_DATASETNAME_FIELD_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(instance.getIndexName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXNAME_FIELD_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(instance.getIndexType().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX, fieldValue);

        // write field 4
        primaryKeyListBuilder
                .reset((AOrderedListType) MetadataRecordTypes.INDEX_RECORDTYPE.getFieldTypes()[MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX]);
        this.searchKey = instance.getKeyFieldNames();
        for (List<String> field : this.searchKey) {
            listBuilder.reset(stringList);
            for (String subField : field) {
                itemValue.reset();
                aString.setValue(subField);
                stringSerde.serialize(aString, itemValue.getDataOutput());
                listBuilder.addItem(itemValue);
            }
            itemValue.reset();
            listBuilder.write(itemValue.getDataOutput(), true);
            primaryKeyListBuilder.addItem(itemValue);
        }
        fieldValue.reset();
        primaryKeyListBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_SEARCHKEY_FIELD_INDEX, fieldValue);

        // write field 5
        fieldValue.reset();
        if (instance.isPrimaryIndex()) {
            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());
        } else {
            booleanSerde.serialize(ABoolean.FALSE, fieldValue.getDataOutput());
        }
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_ISPRIMARY_FIELD_INDEX, fieldValue);

        // write field 6
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_TIMESTAMP_FIELD_INDEX, fieldValue);

        // write field 7
        fieldValue.reset();
        intSerde.serialize(new AInt32(instance.getPendingOp()), fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.INDEX_ARECORD_PENDINGOP_FIELD_INDEX, fieldValue);

        // write optional field 8
        IndexType indexType = instance.getIndexType();
        IndexTypeProperty indexTypeProperty = instance.getIndexTypeProperty();
        if (indexType == IndexType.LENGTH_PARTITIONED_NGRAM_INVIX
                || indexType == IndexType.SINGLE_PARTITION_NGRAM_INVIX) {
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(GRAM_LENGTH_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            intSerde.serialize(new AInt32(indexTypeProperty.gramLength), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
        }

        //write optional fields for cell-based spatial index types
        if (indexType == IndexType.SIF || indexType == IndexType.STATIC_HILBERT_BTREE) {
            //bottomLeftX
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(BOTTOM_LEFT_X_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            doubleSerde.serialize(new ADouble(indexTypeProperty.bottomLeftX), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            //bottomLeftY
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(BOTTOM_LEFT_Y_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            doubleSerde.serialize(new ADouble(indexTypeProperty.bottomLeftY), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            //topRightX
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(TOP_RIGHT_X_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            doubleSerde.serialize(new ADouble(indexTypeProperty.topRightX), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            //topRightY
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(TOP_RIGHT_Y_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            doubleSerde.serialize(new ADouble(indexTypeProperty.topRightY), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            //levelDensity
            for (int i = 0; i < IndexTypeProperty.CELL_BASED_SPATIAL_INDEX_MAX_LEVEL; i++) {
                fieldValue.reset();
                nameValue.reset();
                aString.setValue(LEVEL_DENSITY_FIELD_NAME_PREFIX+i);
                stringSerde.serialize(aString, nameValue.getDataOutput());
                shortSerde.serialize(new AInt16(indexTypeProperty.levelDensity[i]), fieldValue.getDataOutput());
                try {
                    recordBuilder.addField(nameValue, fieldValue);
                } catch (AsterixException e) {
                    throw new MetadataException(e);
                }
            }

            //cellsPerObject
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(CELLS_PER_OBJECT_FIELD_NAME);
            stringSerde.serialize(aString, nameValue.getDataOutput());
            intSerde.serialize(new AInt32(indexTypeProperty.cellsPerObject), fieldValue.getDataOutput());
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
        }

        if (instance.isEnforcingKeyFileds()) {
            // write optional field 9
            OrderedListBuilder typeListBuilder = new OrderedListBuilder();
            typeListBuilder.reset(new AOrderedListType(BuiltinType.ASTRING, null));
            ArrayBackedValueStorage nameValue = new ArrayBackedValueStorage();
            nameValue.reset();
            aString.setValue(INDEX_SEARCHKEY_TYPE_FIELD_NAME);

            stringSerde.serialize(aString, nameValue.getDataOutput());

            this.searchKeyType = instance.getKeyFieldTypes();
            for (IAType type : this.searchKeyType) {
                itemValue.reset();
                aString.setValue(type.getTypeName());
                stringSerde.serialize(aString, itemValue.getDataOutput());
                typeListBuilder.addItem(itemValue);
            }
            fieldValue.reset();
            typeListBuilder.write(fieldValue.getDataOutput(), true);
            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }

            // write optional field 10
            fieldValue.reset();
            nameValue.reset();
            aString.setValue(INDEX_ISENFORCED_FIELD_NAME);

            stringSerde.serialize(aString, nameValue.getDataOutput());

            booleanSerde.serialize(ABoolean.TRUE, fieldValue.getDataOutput());

            try {
                recordBuilder.addField(nameValue, fieldValue);
            } catch (AsterixException e) {
                throw new MetadataException(e);
            }
        }

        // write record
        try {
            recordBuilder.write(tupleBuilder.getDataOutput(), true);
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
