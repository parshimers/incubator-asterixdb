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
import java.util.List;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;
import edu.uci.ics.asterix.om.pointables.PointableAllocator;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
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
            private final AString fieldName = new AString("name");
            private final AString typeName = new AString("type");
            private final AString isOpenName = new AString("is-open");
            private final AString nestedName = new AString("nested");

            protected AMutableString aString = new AMutableString("");
            @SuppressWarnings("unchecked")
            protected ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ASTRING);

            private DataOutput out = output.getDataOutput();

            private OrderedListBuilder orderedListBuilder = new OrderedListBuilder();
            private final AOrderedListType listType = new AOrderedListType(BuiltinType.ANY, "fields");

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);

            private final PointableAllocator pa = new PointableAllocator();
            private ARecordPointable recordPointable;

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

                recordPointable = (ARecordPointable) pa.allocateRecordValue(recordType);
                recordPointable.set(outInput0.getByteArray(), outInput0.getStartOffset(), outInput0.getLength());

                try {
                    orderedListBuilder.reset(listType);
                    processRecord(recordPointable, recordType);
                    orderedListBuilder.write(out, true);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (AsterixException e) {
                    e.printStackTrace();
                }
            }

            public void processRecord(ARecordPointable recordAccessor, ARecordType recType) throws IOException,
                    AsterixException {
                List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
                List<IVisitablePointable> fieldTags = recordAccessor.getFieldTypeTags();
                List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

                ArrayBackedValueStorage fieldAbvs = new ArrayBackedValueStorage();
                ArrayBackedValueStorage valueAbvs = new ArrayBackedValueStorage();
                ArrayBackedValueStorage itemValue = new ArrayBackedValueStorage();
                IARecordBuilder fieldRecordBuilder = new RecordBuilder();
                fieldRecordBuilder.reset(null);
                String fieldTypeName = null;
                for (int i = 0; i < fieldNames.size() - 1; i++) {
                    itemValue.reset();
                    fieldRecordBuilder.init();

                    // write name
                    fieldAbvs.reset();
                    stringSerde.serialize(fieldName, fieldAbvs.getDataOutput());
                    valueAbvs.reset();
                    aString.setValue(recType.getFieldNames()[i]);
                    stringSerde.serialize(aString, valueAbvs.getDataOutput());
                    fieldRecordBuilder.addField(fieldAbvs, valueAbvs);

                    // write type
                    fieldAbvs.reset();
                    stringSerde.serialize(typeName, fieldAbvs.getDataOutput());
                    valueAbvs.reset();
                    if (recType.getFieldTypes()[i].getTypeName() == null) {
                        fieldTypeName = "UNKNOWN";
                    } else {
                        fieldTypeName = recType.getFieldTypes()[i].getTypeName();
                    }
                    aString.setValue(fieldTypeName);
                    stringSerde.serialize(aString, valueAbvs.getDataOutput());
                    fieldRecordBuilder.addField(fieldAbvs, valueAbvs);

                    // write record
                    fieldRecordBuilder.write(itemValue.getDataOutput(), true);

                    // add item to the list of fields
                    orderedListBuilder.addItem(itemValue);
                }
            }
        };
    }
}
