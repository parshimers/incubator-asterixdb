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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.IAsterixListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldsEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ARecordType recordType;

    private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();

    public GetRecordFieldsEvalFactory(ICopyEvaluatorFactory recordEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {
            private final AString fieldName = new AString("field-name");
            private final AString typeName = new AString("field-type");
            private final AString isOpenName = new AString("is-open");
            private final AString nestedName = new AString("nested");
            private final AString listName = new AString("list");

            private final Queue<ArrayBackedValueStorage> baaosPool = new ArrayDeque<ArrayBackedValueStorage>();
            private final Queue<IARecordBuilder> recordBuilderPool = new ArrayDeque<IARecordBuilder>();
            private final Queue<IAsterixListBuilder> orderedListBuilderPool = new ArrayDeque<IAsterixListBuilder>();
            private final PointableAllocator pa = new PointableAllocator();

            private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "fields");
            private final AMutableString aString = new AMutableString("");
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            @SuppressWarnings("unchecked")
            protected final ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ASTRING);
            @SuppressWarnings("unchecked")
            protected final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ABOOLEAN);

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);

            private DataOutput out = output.getDataOutput();
            private ATypeTag tag;
            private ATypeTag innerTag;
            private byte tagId;
            private AString currentFieldName;
            private IAType fieldType;

            private final ARecordType openType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            {
                recordType = recordType.deepCopy(recordType);
            }

            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                outInput0.reset();
                eval0.evaluate(tuple);

                if (outInput0.getByteArray()[0] == SER_NULL_TYPE_TAG) {
                    try {
                        nullSerde.serialize(ANull.NULL, out);
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }

                if (outInput0.getByteArray()[0] != SER_RECORD_TYPE_TAG) {
                    throw new AlgebricksException("Field accessor is not defined for values of type "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(outInput0.getByteArray()[0]));
                }

                ARecordPointable recordPointable = (ARecordPointable) pa.allocateRecordValue(recordType);
                recordPointable.set(outInput0.getByteArray(), outInput0.getStartOffset(), outInput0.getLength());

                try {
                    processRecord(recordPointable, recordType, out);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (AsterixException e) {
                    e.printStackTrace();
                }
            }

            public void processRecord(ARecordPointable recordAccessor, ARecordType recType, DataOutput out)
                    throws IOException, AsterixException, AlgebricksException {
                List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
                List<IVisitablePointable> fieldTags = recordAccessor.getFieldTypeTags();
                List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
                ArrayBackedValueStorage valueAbvs = getTempBuffer();
                ArrayBackedValueStorage itemValue = getTempBuffer();

                OrderedListBuilder orderedListBuilder = getOrderedListBuilder();
                orderedListBuilder.reset(listType);
                IARecordBuilder fieldRecordBuilder = getRecordBuilder();
                fieldRecordBuilder.reset(null);
                for (int i = 0; i < fieldNames.size(); i++) {
                    itemValue.reset();
                    fieldRecordBuilder.init();

                    // write name
                    fieldAbvs.reset();
                    stringSerde.serialize(fieldName, fieldAbvs.getDataOutput());
                    fieldRecordBuilder.addField(fieldAbvs, fieldNames.get(i));

                    // write type
                    tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(fieldTags.get(i).getByteArray()[fieldTags
                            .get(i).getStartOffset()]);
                    addFieldType(tag, fieldRecordBuilder);

                    // write open
                    fieldAbvs.reset();
                    stringSerde.serialize(isOpenName, fieldAbvs.getDataOutput());
                    valueAbvs.reset();
                    currentFieldName = stringSerde
                            .deserialize(new DataInputStream(new ByteArrayInputStream(fieldNames.get(i).getByteArray(),
                                    fieldNames.get(i).getStartOffset(), fieldNames.get(i).getLength())));
                    fieldType = recType.getFieldType(currentFieldName.getStringValue());
                    if (fieldType == null) {
                        booleanSerde.serialize(ABoolean.TRUE, valueAbvs.getDataOutput());
                    } else {
                        booleanSerde.serialize(ABoolean.FALSE, valueAbvs.getDataOutput());
                    }
                    fieldRecordBuilder.addField(fieldAbvs, valueAbvs);

                    // write nested or list types
                    if (tag == ATypeTag.RECORD) {
                        addNestedField(fieldValues.get(i), fieldType, fieldRecordBuilder);
                    } else if (tag == ATypeTag.ORDEREDLIST) {
                        fieldAbvs.reset();
                        stringSerde.serialize(listName, fieldAbvs.getDataOutput());
                        valueAbvs.reset();
                        processListValue(fieldValues.get(i), fieldType, valueAbvs.getDataOutput());
                        fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
                    }

                    // write record
                    fieldRecordBuilder.write(itemValue.getDataOutput(), true);

                    // add item to the list of fields
                    orderedListBuilder.addItem(itemValue);
                }
                orderedListBuilder.write(out, true);

                // Return memory.
                returnRecordBuilder(fieldRecordBuilder);
                returnOrderedListBuilder(orderedListBuilder);
                returnTempBuffer(fieldAbvs);
                returnTempBuffer(valueAbvs);
                returnTempBuffer(itemValue);
            }

            private void addNestedField(IVisitablePointable recordArg, IAType fieldType,
                    IARecordBuilder fieldRecordBuilder) throws HyracksDataException, AlgebricksException, IOException,
                    AsterixException {
                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
                ArrayBackedValueStorage valueAbvs = getTempBuffer();

                // Name
                fieldAbvs.reset();
                stringSerde.serialize(nestedName, fieldAbvs.getDataOutput());
                // Value
                valueAbvs.reset();
                ARecordType newType;
                if (fieldType == null) {
                    newType = openType;
                } else {
                    newType = (ARecordType) fieldType;
                }
                newType = newType.deepCopy(newType);
                ARecordPointable recordP = (ARecordPointable) pa.allocateRecordValue(newType);
                recordP.set(recordArg.getByteArray(), recordArg.getStartOffset(), recordArg.getLength());
                processRecord(recordP, newType, valueAbvs.getDataOutput());
                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);

                returnTempBuffer(fieldAbvs);
                returnTempBuffer(valueAbvs);
            }

            private void processListValue(IVisitablePointable listArg, IAType fieldType, DataOutput out)
                    throws AsterixException, IOException, AlgebricksException {
                ArrayBackedValueStorage itemValue = getTempBuffer();
                IARecordBuilder listRecordBuilder = getRecordBuilder();

                AListPointable list = (AListPointable) pa.allocateListValue(fieldType);
                list.set(listArg.getByteArray(), listArg.getStartOffset(), listArg.getLength());

                OrderedListBuilder innerListBuilder = getOrderedListBuilder();
                innerListBuilder.reset(listType);

                listRecordBuilder.reset(null);
                for (int l = 0; l < list.getItemTags().size(); l++) {
                    itemValue.reset();
                    listRecordBuilder.init();

                    tagId = list.getItemTags().get(l).getByteArray()[list.getItemTags().get(l).getStartOffset()];
                    innerTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tagId);
                    addFieldType(innerTag, listRecordBuilder);

                    if (innerTag == ATypeTag.RECORD) {
                        AbstractCollectionType act = (AbstractCollectionType) fieldType;
                        addNestedField(list.getItems().get(l), act.getItemType(), listRecordBuilder);
                    }

                    listRecordBuilder.write(itemValue.getDataOutput(), true);
                    innerListBuilder.addItem(itemValue);
                }
                innerListBuilder.write(out, true);

                returnRecordBuilder(listRecordBuilder);
                returnTempBuffer(itemValue);
            }

            private void addFieldType(ATypeTag tag, IARecordBuilder fieldRecordBuilder) throws HyracksDataException,
                    AsterixException {
                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
                ArrayBackedValueStorage valueAbvs = getTempBuffer();

                // Name
                fieldAbvs.reset();
                stringSerde.serialize(typeName, fieldAbvs.getDataOutput());
                // Value
                valueAbvs.reset();
                aString.setValue(tag.toString());
                stringSerde.serialize(aString, valueAbvs.getDataOutput());
                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);

                returnTempBuffer(fieldAbvs);
                returnTempBuffer(valueAbvs);
            }

            private IARecordBuilder getRecordBuilder() {
                RecordBuilder recBuilder = (RecordBuilder) recordBuilderPool.poll();
                if (recBuilder != null) {
                    return recBuilder;
                } else {
                    return new RecordBuilder();
                }
            }

            private void returnRecordBuilder(IARecordBuilder recBuilder) {
                this.recordBuilderPool.add(recBuilder);
            }

            private OrderedListBuilder getOrderedListBuilder() {
                OrderedListBuilder orderedListBuilder = (OrderedListBuilder) orderedListBuilderPool.poll();
                if (orderedListBuilder != null) {
                    return orderedListBuilder;
                } else {
                    return new OrderedListBuilder();
                }
            }

            private void returnOrderedListBuilder(OrderedListBuilder orderedListBuilder) {
                this.orderedListBuilderPool.add(orderedListBuilder);
            }

            private ArrayBackedValueStorage getTempBuffer() {
                ArrayBackedValueStorage tmpBaaos = baaosPool.poll();
                if (tmpBaaos != null) {
                    return tmpBaaos;
                } else {
                    return new ArrayBackedValueStorage();
                }
            }

            private void returnTempBuffer(ArrayBackedValueStorage tempBaaos) {
                baaosPool.add(tempBaaos);
            }

        };
    }
}
