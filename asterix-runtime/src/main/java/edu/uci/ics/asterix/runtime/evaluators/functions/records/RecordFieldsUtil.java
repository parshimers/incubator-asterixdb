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
package edu.uci.ics.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class RecordFieldsUtil {

    private final static byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();
    private final static byte SER_ORDERED_LIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
    private final static byte SER_UNORDERED_LIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    private final static AString fieldName = new AString("field-name");
    private final static AString typeName = new AString("field-type");
    private final static AString isOpenName = new AString("is-open");
    private final static AString nestedName = new AString("nested");
    private final static AString listName = new AString("list");

    private final static AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "fields");
    private final static AMutableString aString = new AMutableString("");
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    protected final static ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final static ARecordType openType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;

    public static void processRecord(ARecordPointable recordAccessor, ARecordType recType, DataOutput out, int level)
            throws IOException, AsterixException, AlgebricksException {
        ArrayBackedValueStorage itemValue = getTempBuffer();
        ArrayBackedValueStorage fieldName = getTempBuffer();

        OrderedListBuilder orderedListBuilder = getOrderedListBuilder();
        orderedListBuilder.reset(listType);
        IARecordBuilder fieldRecordBuilder = getRecordBuilder();
        fieldRecordBuilder.reset(null);

        for (int i = 0; i < recordAccessor.getSchemeFieldCount(recType); ++i) {
            itemValue.reset();
            fieldRecordBuilder.init();

            // write name
            fieldName.reset();
            recordAccessor.getClosedFieldName(recType, i, fieldName.getDataOutput());
            RecordFieldsUtil.addNameField(fieldName, fieldRecordBuilder);

            // write type
            byte tag = recordAccessor.getClosedFieldTag(recType, i);
            if (recordAccessor.isClosedFieldNull(recType, i)) {
                //                tag = ATypeTag.NULL.serialize();
            }
            RecordFieldsUtil.addFieldType(tag, fieldRecordBuilder);

            // write open
            RecordFieldsUtil.addIsOpenField(false, fieldRecordBuilder);

            // write nested or list types
            if (tag == SER_RECORD_TYPE_TAG || tag == SER_ORDERED_LIST_TYPE_TAG || tag == SER_UNORDERED_LIST_TYPE_TAG) {
                if (!recordAccessor.isClosedFieldNull(recType, i)) {
                    IAType fieldType = recordAccessor.getClosedFieldType(recType, i);
                    ArrayBackedValueStorage tmpValue = getTempBuffer();
                    tmpValue.reset();
                    recordAccessor.getClosedFieldValue(recType, i, tmpValue.getDataOutput());
                    if (tag == SER_RECORD_TYPE_TAG) {
                        addNestedField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                    } else if (tag == SER_ORDERED_LIST_TYPE_TAG || tag == SER_UNORDERED_LIST_TYPE_TAG) {
                        addListField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                    }
                }
            }

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            orderedListBuilder.addItem(itemValue);
        }
        for (int i = recordAccessor.getOpenFieldCount(recType) - 1; i >= 0; --i) {
            itemValue.reset();
            fieldRecordBuilder.init();

            // write name
            fieldName.reset();
            recordAccessor.getOpenFieldName(recType, i, fieldName.getDataOutput());
            RecordFieldsUtil.addNameField(fieldName, fieldRecordBuilder);

            // write type
            byte tag = recordAccessor.getOpenFieldTag(recType, i);
            RecordFieldsUtil.addFieldType(tag, fieldRecordBuilder);

            // write open
            RecordFieldsUtil.addIsOpenField(true, fieldRecordBuilder);

            // write nested or list types
            if (tag == SER_RECORD_TYPE_TAG || tag == SER_ORDERED_LIST_TYPE_TAG || tag == SER_UNORDERED_LIST_TYPE_TAG) {
                IAType fieldType = null;
                ArrayBackedValueStorage tmpValue = getTempBuffer();
                tmpValue.reset();
                recordAccessor.getOpenFieldValue(recType, i, tmpValue.getDataOutput());
                if (tag == SER_RECORD_TYPE_TAG) {
                    addNestedField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                } else if (tag == SER_ORDERED_LIST_TYPE_TAG || tag == SER_UNORDERED_LIST_TYPE_TAG) {
                    addListField(tmpValue, fieldType, fieldRecordBuilder, level + 1);
                }
            }

            // write record
            fieldRecordBuilder.write(itemValue.getDataOutput(), true);

            // add item to the list of fields
            orderedListBuilder.addItem(itemValue);
        }
        orderedListBuilder.write(out, true);
    }

    public static void addNameField(IValueReference nameArg, IARecordBuilder fieldRecordBuilder)
            throws HyracksDataException, AsterixException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();

        fieldAbvs.reset();
        stringSerde.serialize(fieldName, fieldAbvs.getDataOutput());
        fieldRecordBuilder.addField(fieldAbvs, nameArg);
    }

    public static void addFieldType(byte tagId, IARecordBuilder fieldRecordBuilder) throws HyracksDataException,
            AsterixException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(typeName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tagId);
        AMutableString aString = new AMutableString("");
        //        @SuppressWarnings("unchecked")
        //        ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
        //                .getSerializerDeserializer(BuiltinType.ASTRING);
        aString.setValue(tag.toString());
        stringSerde.serialize(aString, valueAbvs.getDataOutput());
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public static void addIsOpenField(boolean isOpen, IARecordBuilder fieldRecordBuilder) throws HyracksDataException,
            AsterixException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(isOpenName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        if (isOpen) {
            booleanSerde.serialize(ABoolean.TRUE, valueAbvs.getDataOutput());
        } else {
            booleanSerde.serialize(ABoolean.FALSE, valueAbvs.getDataOutput());
        }
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public static void addListField(IValueReference listArg, IAType fieldType, IARecordBuilder fieldRecordBuilder,
            int level) throws AsterixException, IOException, AlgebricksException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(listName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        processListValue(listArg, fieldType, valueAbvs.getDataOutput(), level);
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public static void addNestedField(IValueReference recordArg, IAType fieldType, IARecordBuilder fieldRecordBuilder,
            int level) throws HyracksDataException, AlgebricksException, IOException, AsterixException {
        ArrayBackedValueStorage fieldAbvs = getTempBuffer();
        ArrayBackedValueStorage valueAbvs = getTempBuffer();

        // Name
        fieldAbvs.reset();
        stringSerde.serialize(nestedName, fieldAbvs.getDataOutput());
        // Value
        valueAbvs.reset();
        ARecordType newType;
        if (fieldType == null) {
            newType = openType.deepCopy(openType);
        } else {
            newType = ((ARecordType) fieldType).deepCopy((ARecordType) fieldType);
        }
        ARecordPointable recordP = new ARecordPointable();
        recordP.set(recordArg);
        processRecord(recordP, (ARecordType) newType, valueAbvs.getDataOutput(), level);
        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
    }

    public static void processListValue(IValueReference listArg, IAType fieldType, DataOutput out, int level)
            throws AsterixException, IOException, AlgebricksException {
        ArrayBackedValueStorage itemValue = getTempBuffer();
        IARecordBuilder listRecordBuilder = getRecordBuilder();

        AListPointable list = new AListPointable();
        list.set(listArg);

        OrderedListBuilder innerListBuilder = getOrderedListBuilder();
        innerListBuilder.reset(listType);

        listRecordBuilder.reset(null);
        AbstractCollectionType act = (AbstractCollectionType) fieldType;
        for (int l = 0; l < list.getItemCount(); l++) {
            itemValue.reset();
            listRecordBuilder.init();

            byte tagId = list.getItemTag(act, l);
            addFieldType(tagId, listRecordBuilder);

            if (tagId == SER_RECORD_TYPE_TAG) {
                ArrayBackedValueStorage tmpAbvs = getTempBuffer();
                list.getItemValue(act, l, tmpAbvs.getDataOutput());
                addNestedField(tmpAbvs, act.getItemType(), listRecordBuilder, level + 1);
            }

            listRecordBuilder.write(itemValue.getDataOutput(), true);
            innerListBuilder.addItem(itemValue);
        }
        innerListBuilder.write(out, true);
    }

    private static IARecordBuilder getRecordBuilder() {
        //        return (RecordBuilder) recordBuilderPool.allocate("record");
        return new RecordBuilder();
    }

    private static OrderedListBuilder getOrderedListBuilder() {
        //        return (OrderedListBuilder) listBuilderPool.allocate("ordered");
        return new OrderedListBuilder();
    }

    private static ArrayBackedValueStorage getTempBuffer() {
        //        return (ArrayBackedValueStorage) abvsBuilderPool.allocate("buffer");
        return new ArrayBackedValueStorage();
    }
}
