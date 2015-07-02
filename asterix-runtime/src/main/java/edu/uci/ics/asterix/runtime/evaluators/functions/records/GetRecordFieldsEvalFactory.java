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
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.builders.AbvsBuilderFactory;
import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.IAsterixListBuilder;
import edu.uci.ics.asterix.builders.ListBuilderFactory;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilderFactory;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.ARecordVisitablePointable;
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
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldsEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ARecordType recordType;

    private final byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
    private final byte SER_RECORD_TYPE_TAG = ATypeTag.RECORD.serialize();
//    private final byte SER_ORDERED_LIST_TYPE_TAG = ATypeTag.ORDEREDLIST.serialize();
//    private final byte SER_UNORDERED_LIST_TYPE_TAG = ATypeTag.UNORDEREDLIST.serialize();

    public GetRecordFieldsEvalFactory(ICopyEvaluatorFactory recordEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {
//            private final AString fieldName = new AString("field-name");
//            private final AString typeName = new AString("field-type");
//            private final AString isOpenName = new AString("is-open");
//            private final AString nestedName = new AString("nested");
//            private final AString listName = new AString("list");
//
//            private List<AListVisitablePointable> listPointable = new ArrayList<AListVisitablePointable>();
//            private List<ARecordVisitablePointable> recordPointable = new ArrayList<ARecordVisitablePointable>();
//
//            private IObjectPool<IARecordBuilder, String> recordBuilderPool = new ListObjectPool<IARecordBuilder, String>(
//                    new RecordBuilderFactory());
//            private IObjectPool<IAsterixListBuilder, String> listBuilderPool = new ListObjectPool<IAsterixListBuilder, String>(
//                    new ListBuilderFactory());
//            private IObjectPool<IMutableValueStorage, String> abvsBuilderPool = new ListObjectPool<IMutableValueStorage, String>(
//                    new AbvsBuilderFactory());
//            private final PointableAllocator pa = new PointableAllocator();
//
//            private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "fields");
//            private final AMutableString aString = new AMutableString("");
            @SuppressWarnings("unchecked")
            private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
//            @SuppressWarnings("unchecked")
//            protected final ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
//                    .getSerializerDeserializer(BuiltinType.ASTRING);
//            @SuppressWarnings("unchecked")
//            protected final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
//                    .getSerializerDeserializer(BuiltinType.ABOOLEAN);

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private DataOutput out = output.getDataOutput();

//            private final ARecordType openType = DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
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

                //              ARecordVisitablePointable recordPointable = new ARecordVisitablePointable(recordType);//(ARecordPointable) pa.allocateRecordValue(recordType);
                ARecordPointable recordPointable = new ARecordPointable();//(ARecordPointable) pa.allocateRecordValue(recordType);
                recordPointable
                        .set(outInput0.getByteArray(), outInput0.getStartOffset(), outInput0.getLength());

                try {
                    RecordFieldsUtil.processRecord(recordPointable, recordType, out, 0);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (AsterixException e) {
                    e.printStackTrace();
                }
            }
//
//            public void processRecord(ARecordVisitablePointable recordAccessor, ARecordType recType, DataOutput out,
//                    int level) throws IOException, AsterixException, AlgebricksException {
//                List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
//                List<IVisitablePointable> fieldTags = recordAccessor.getFieldTypeTags();
//                List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();
//
//                ArrayBackedValueStorage itemValue = getTempBuffer();
//
//                OrderedListBuilder orderedListBuilder = getOrderedListBuilder();
//                orderedListBuilder.reset(listType);
//                IARecordBuilder fieldRecordBuilder = getRecordBuilder();
//                fieldRecordBuilder.reset(null);
//                for (int i = 0; i < fieldNames.size(); i++) {
//                    itemValue.reset();
//                    fieldRecordBuilder.init();
//
//                    // write name
//                    RecordFieldsUtil.addNameField(fieldNames.get(i), fieldRecordBuilder);
//
//                    // write type
//                    byte tag = fieldTags.get(i).getByteArray()[fieldTags.get(i).getStartOffset()];
//                    RecordFieldsUtil.addFieldType(tag, fieldRecordBuilder);
//
//                    // write open
//                    int index = recType.findFieldPosition(fieldNames.get(i).getByteArray(), fieldNames.get(i)
//                            .getStartOffset() + 1, fieldNames.get(i).getLength());
//                    RecordFieldsUtil.addIsOpenField((index == -1), fieldRecordBuilder);
//
//                    // write nested or list types
//                    if (tag == SER_RECORD_TYPE_TAG || tag == SER_ORDERED_LIST_TYPE_TAG
//                            || tag == SER_UNORDERED_LIST_TYPE_TAG) {
//                        IAType fieldType = null;
//                        if (index > -1) {
//                            fieldType = recType.getFieldTypes()[index];
//                        }
//                        if (tag == SER_RECORD_TYPE_TAG) {
//                            addNestedField(fieldValues.get(i), fieldType, fieldRecordBuilder, level + 1);
//                        } else if (tag == SER_ORDERED_LIST_TYPE_TAG || tag == SER_UNORDERED_LIST_TYPE_TAG) {
//                            addListField(fieldValues.get(i), fieldType, fieldRecordBuilder, level + 1);
//                        }
//                    }
//
//                    // write record
//                    fieldRecordBuilder.write(itemValue.getDataOutput(), true);
//
//                    // add item to the list of fields
//                    orderedListBuilder.addItem(itemValue);
//                }
//                orderedListBuilder.write(out, true);
//            }
//
//            //            private void addNameField(IVisitablePointable nameArg, IARecordBuilder fieldRecordBuilder)
//            //                    throws HyracksDataException, AsterixException {
//            //                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
//            //
//            //                fieldAbvs.reset();
//            //                stringSerde.serialize(fieldName, fieldAbvs.getDataOutput());
//            //                fieldRecordBuilder.addField(fieldAbvs, nameArg);
//            //            }
//            //
//            //            private void addFieldType(byte tagId, IARecordBuilder fieldRecordBuilder) throws HyracksDataException,
//            //                    AsterixException {
//            //                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
//            //                ArrayBackedValueStorage valueAbvs = getTempBuffer();
//            //
//            //                // Name
//            //                fieldAbvs.reset();
//            //                stringSerde.serialize(typeName, fieldAbvs.getDataOutput());
//            //                // Value
//            //                valueAbvs.reset();
//            //                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tagId);
//            //                aString.setValue(tag.toString());
//            //                stringSerde.serialize(aString, valueAbvs.getDataOutput());
//            //                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
//            //            }
//            //
//            //            private void addIsOpenField(boolean isOpen, IARecordBuilder fieldRecordBuilder)
//            //                    throws HyracksDataException, AsterixException {
//            //                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
//            //                ArrayBackedValueStorage valueAbvs = getTempBuffer();
//            //
//            //                // Name
//            //                fieldAbvs.reset();
//            //                stringSerde.serialize(isOpenName, fieldAbvs.getDataOutput());
//            //                // Value
//            //                valueAbvs.reset();
//            //                if (isOpen) {
//            //                    booleanSerde.serialize(ABoolean.TRUE, valueAbvs.getDataOutput());
//            //                } else {
//            //                    booleanSerde.serialize(ABoolean.FALSE, valueAbvs.getDataOutput());
//            //                }
//            //                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
//            //            }
//
//            private void addListField(IVisitablePointable listArg, IAType fieldType,
//                    IARecordBuilder fieldRecordBuilder, int level) throws AsterixException, IOException,
//                    AlgebricksException {
//                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
//                ArrayBackedValueStorage valueAbvs = getTempBuffer();
//
//                // Name
//                fieldAbvs.reset();
//                stringSerde.serialize(listName, fieldAbvs.getDataOutput());
//                // Value
//                valueAbvs.reset();
//                processListValue(listArg, fieldType, valueAbvs.getDataOutput(), level);
//                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
//            }
//
//            private void addNestedField(IVisitablePointable recordArg, IAType fieldType,
//                    IARecordBuilder fieldRecordBuilder, int level) throws HyracksDataException, AlgebricksException,
//                    IOException, AsterixException {
//                ArrayBackedValueStorage fieldAbvs = getTempBuffer();
//                ArrayBackedValueStorage valueAbvs = getTempBuffer();
//
//                // Name
//                fieldAbvs.reset();
//                stringSerde.serialize(nestedName, fieldAbvs.getDataOutput());
//                // Value
//                valueAbvs.reset();
//                ARecordType newType;
//                if (fieldType == null) {
//                    newType = openType.deepCopy(openType);
//                } else {
//                    newType = ((ARecordType) fieldType).deepCopy((ARecordType) fieldType);
//                }
//
//                while (recordPointable.size() < level + 1) {
//                    recordPointable.add(new ARecordVisitablePointable(newType));
//                }
//
//                recordPointable.set(level, new ARecordVisitablePointable(newType));
//                //                recordPointable.get(level).set(recordArg);
//                ARecordVisitablePointable recordP = recordPointable.get(level);
//
//                //                ARecordPointable recordP = (ARecordPointable) pa.allocateRecordValue(newType);
//                recordP.set(recordArg);
//                processRecord(recordP, newType, valueAbvs.getDataOutput(), level);
//                fieldRecordBuilder.addField(fieldAbvs, valueAbvs);
//            }
//
//            private void processListValue(IVisitablePointable listArg, IAType fieldType, DataOutput out, int level)
//                    throws AsterixException, IOException, AlgebricksException {
//                ArrayBackedValueStorage itemValue = getTempBuffer();
//                IARecordBuilder listRecordBuilder = getRecordBuilder();
//
//                while (listPointable.size() < level + 1) {
//                    listPointable.add(new AListVisitablePointable((AbstractCollectionType) fieldType));
//                }
//
//                //                AListPointable list = (AListPointable) pa.allocateListValue(fieldType);
//                listPointable.set(level, new AListVisitablePointable((AbstractCollectionType) fieldType));
//                listPointable.get(level).set(listArg);
//                AListVisitablePointable list = listPointable.get(level);
//
//                OrderedListBuilder innerListBuilder = getOrderedListBuilder();
//                innerListBuilder.reset(listType);
//
//                listRecordBuilder.reset(null);
//                for (int l = 0; l < list.getItemTags().size(); l++) {
//                    itemValue.reset();
//                    listRecordBuilder.init();
//
//                    byte tagId = list.getItemTags().get(l).getByteArray()[list.getItemTags().get(l).getStartOffset()];
//                    RecordFieldsUtil.addFieldType(tagId, listRecordBuilder);
//
//                    if (tagId == SER_RECORD_TYPE_TAG) {
//                        AbstractCollectionType act = (AbstractCollectionType) fieldType;
//                        addNestedField(list.getItems().get(l), act.getItemType(), listRecordBuilder, level + 1);
//                    }
//
//                    listRecordBuilder.write(itemValue.getDataOutput(), true);
//                    innerListBuilder.addItem(itemValue);
//                }
//                innerListBuilder.write(out, true);
//            }
//
//            private IARecordBuilder getRecordBuilder() {
//                return (RecordBuilder) recordBuilderPool.allocate("record");
//            }
//
//            private OrderedListBuilder getOrderedListBuilder() {
//                return (OrderedListBuilder) listBuilderPool.allocate("ordered");
//            }
//
//            private ArrayBackedValueStorage getTempBuffer() {
//                return (ArrayBackedValueStorage) abvsBuilderPool.allocate("buffer");
//            }
        };
    }
}
