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
package edu.uci.ics.asterix.metadata.external;

import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;

@SuppressWarnings("rawtypes")
public class FilesIndexDescription {
    public final static int FILE_INDEX_TUPLE_SIZE = 2;
    public final static int FILE_KEY_INDEX = 0;
    public final static int FILE_KEY_SIZE = 1;
    public final static int FILE_PAYLOAD_INDEX = 1;
    public static RecordDescriptor FILE_INDEX_RECORD_DESCRIPTOR;
    public static RecordDescriptor FILE_BUDDY_BTREE_RECORD_DESCRIPTOR;
    public final static String[] payloadFieldNames = { "FileName", "FileSize", "FileModDate" };
    public final static IAType[] payloadFieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.ADATETIME };
    public static ARecordType EXTERNAL_FILE_RECORD_TYPE;
    public static ISerializerDeserializer EXTERNAL_FILE_RECORD_SERDE;
    public static final ISerializerDeserializer[] EXTERNAL_FILE_BUDDY_BTREE_FIELDS = new ISerializerDeserializer[1];
    public static final ITypeTraits[] EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS = new ITypeTraits[1];
    public static final ISerializerDeserializer[] EXTERNAL_FILE_TUPLE_FIELDS = new ISerializerDeserializer[FILE_INDEX_TUPLE_SIZE];
    public static final ITypeTraits[] EXTERNAL_FILE_INDEX_TYPE_TRAITS = new ITypeTraits[FILE_INDEX_TUPLE_SIZE];
    public static final IBinaryComparatorFactory[] FILES_INDEX_COMP_FACTORIES = new IBinaryComparatorFactory[] { AqlBinaryComparatorFactoryProvider.INSTANCE
            .getBinaryComparatorFactory(BuiltinType.AINT32, true) };
    public static final int[] BLOOM_FILTER_FIELDS = { 0 };
    public static final int EXTERNAL_FILE_NAME_FIELD_INDEX = 0;
    public static final int EXTERNAL_FILE_SIZE_FIELD_INDEX = 1;
    public static final int EXTERNAL_FILE_MOD_DATE_FIELD_INDEX = 2;
    static {
        try {
            EXTERNAL_FILE_RECORD_TYPE = new ARecordType("ExternalFileRecordType", payloadFieldNames, payloadFieldTypes,
                    true);
            EXTERNAL_FILE_RECORD_SERDE = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(EXTERNAL_FILE_RECORD_TYPE);

            EXTERNAL_FILE_TUPLE_FIELDS[FILE_KEY_INDEX] = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(IndexingConstants.FILE_NUMBER_FIELD_TYPE);
            EXTERNAL_FILE_TUPLE_FIELDS[FILE_PAYLOAD_INDEX] = EXTERNAL_FILE_RECORD_SERDE;
            EXTERNAL_FILE_BUDDY_BTREE_FIELDS[FILE_KEY_INDEX] = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(IndexingConstants.FILE_NUMBER_FIELD_TYPE);

            EXTERNAL_FILE_INDEX_TYPE_TRAITS[FILE_KEY_INDEX] = AqlTypeTraitProvider.INSTANCE
                    .getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE);
            EXTERNAL_FILE_INDEX_TYPE_TRAITS[FILE_PAYLOAD_INDEX] = AqlTypeTraitProvider.INSTANCE
                    .getTypeTrait(EXTERNAL_FILE_RECORD_TYPE);
            EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS[FILE_KEY_INDEX] = AqlTypeTraitProvider.INSTANCE
                    .getTypeTrait(IndexingConstants.FILE_NUMBER_FIELD_TYPE);

            FILE_INDEX_RECORD_DESCRIPTOR = new RecordDescriptor(EXTERNAL_FILE_TUPLE_FIELDS,
                    EXTERNAL_FILE_INDEX_TYPE_TRAITS);

            FILE_BUDDY_BTREE_RECORD_DESCRIPTOR = new RecordDescriptor(EXTERNAL_FILE_BUDDY_BTREE_FIELDS,
                    EXTERNAL_FILE_BUDDY_BTREE_TYPE_TRAITS);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @SuppressWarnings("unchecked")
    public static void getBuddyBTreeTupleFromFileNumber(ArrayTupleReference tuple, ArrayTupleBuilder tupleBuilder,
            AMutableInt32 aInt32) throws IOException, AsterixException {
        tupleBuilder.reset();
        FilesIndexDescription.FILE_BUDDY_BTREE_RECORD_DESCRIPTOR.getFields()[0].serialize(aInt32,
                tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
    }
}
