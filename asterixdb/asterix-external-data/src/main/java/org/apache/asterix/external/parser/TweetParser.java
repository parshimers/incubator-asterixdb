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
package org.apache.asterix.external.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IARecordBuilder;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.ListBuilderFactory;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.RecordBuilderFactory;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.om.base.AMutablePoint;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringWriter;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

public class TweetParser extends AbstractDataParser implements IRecordDataParser<String> {
    private final IObjectPool<IARecordBuilder, ATypeTag> recordBuilderPool = new ListObjectPool<>(
            new RecordBuilderFactory());
    private final IObjectPool<IAsterixListBuilder, ATypeTag> listBuilderPool = new ListObjectPool<>(
            new ListBuilderFactory());
    private final IObjectPool<IMutableValueStorage, ATypeTag> abvsBuilderPool = new ListObjectPool<>(
            new AbvsBuilderFactory());
    private ARecordType recordType;
    private UTF8StringWriter utf8Writer = new UTF8StringWriter();

    public TweetParser(ARecordType recordType) {
        this.recordType = recordType;
        aPoint = new AMutablePoint(0, 0);
    }

    private void parseUnorderedList(JsonNode jArray, DataOutput output) throws IOException {
        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder orderedList = (OrderedListBuilder) getOrderedListBuilder();

        unorderedListBuilder.reset(null);
        for (int iter1 = 0; iter1 < jArray.size(); iter1++) {
            itemBuffer.reset();
            if (writeField(jArray.get(iter1), orderedListType == null ? null : orderedListType.getItemType(),
                    itemBuffer.getDataOutput())) {
                orderedList.addItem(itemBuffer);
            }
        }
        orderedList.write(output, true);
    }

    private boolean writeField(JsonNode fieldObj, IAType fieldType, DataOutput out) throws IOException {
        boolean writeResult = true;
        IAType chkFieldType;
        chkFieldType = fieldType instanceof AUnionType ? ((AUnionType) fieldType).getActualType() : fieldType;
        try {
            switch (chkFieldType.getTypeTag()) {
                case STRING:
                    out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                    utf8Writer.writeUTF8(fieldObj.asText(), out);
                    break;
                case INT64:
                    aInt64.setValue(fieldObj.asLong());
                    int64Serde.serialize(aInt64, out);
                    break;
                case INT32:
                    out.write(BuiltinType.AINT32.getTypeTag().serialize());
                    out.writeInt(fieldObj.asInt());
                    break;
                case DOUBLE:
                    out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                    out.writeDouble(fieldObj.asDouble());
                    break;
                case BOOLEAN:
                    out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                    out.writeBoolean(fieldObj.asBoolean());
                    break;
                case RECORD:
                    writeRecord(fieldObj, out, (ARecordType) fieldType);
                    break;
                default:
                    writeResult = false;
            }
        } else {
            if (fieldObj.isNull()) {
                nullSerde.serialize(ANull.NULL, out);
            } else if (fieldObj.isInt()) {
                out.write(BuiltinType.AINT32.getTypeTag().serialize());
                out.writeInt(fieldObj.asInt());
            } else if (fieldObj.isBoolean()) {
                out.write(BuiltinType.ABOOLEAN.getTypeTag().serialize());
                out.writeBoolean(fieldObj.asBoolean());
            } else if (fieldObj.isDouble()) {
                out.write(BuiltinType.ADOUBLE.getTypeTag().serialize());
                out.writeDouble(fieldObj.asDouble());
            } else if (fieldObj.isLong()) {
                out.write(BuiltinType.AINT64.getTypeTag().serialize());
                out.writeLong(fieldObj.asLong());
            } else if (fieldObj.isTextual()) {
                out.write(BuiltinType.ASTRING.getTypeTag().serialize());
                utf8Writer.writeUTF8(fieldObj.asText(), out);
            } else if (fieldObj.isArray()) {
                if ((fieldObj).size() != 0) {
                    parseUnorderedList(fieldObj, out);
                } else {
                    writeResult = false;
                }
            } else if (fieldObj.isObject()) {
                if ((fieldObj).size() != 0) {
                    writeRecord(fieldObj, out, null);
                } else {
                    writeResult = false;
                }
            }
        } catch (IOException | JSONException e) {
            throw new HyracksDataException(e);
        }
        return writeResult;
    }

    private boolean writeField(Object fieldObj, IAType fieldType, DataOutput out) throws HyracksDataException {
        return fieldType == null ? writeFieldWithoutFieldType(fieldObj, out)
                : writeFieldWithFieldType(fieldObj, fieldType, out);
    }

    private int checkAttrNameIdx(String[] nameList, String name) {
        int idx = 0;
        if (nameList != null) {
            for (String nln : nameList) {
                if (name.equals(nln)) {
                    return idx;
                }
                idx++;
            }
        }
        return -1;
    }

    public void writeRecord(JsonNode obj, DataOutput out, ARecordType curRecType) throws IOException {
        IAType[] curTypes = null;
        String[] curFNames = null;
        int fieldN;
        int attrIdx;
        boolean writeRecord = false;

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        if (curRecType != null) {
            curTypes = curRecType.getFieldTypes();
            curFNames = curRecType.getFieldNames();
        }

        recBuilder.reset(curRecType);
        recBuilder.init();

        if (curRecType != null && !curRecType.isOpen()) {
            // closed record type
            fieldN = curFNames.length;
            for (int iter1 = 0; iter1 < fieldN; iter1++) {
                fieldValueBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (obj.get(curFNames[iter1]).isNull()) {
                    if (curRecType.isClosedField(curFNames[iter1])) {
                        throw new HyracksDataException("Closed field " + curFNames[iter1] + " has null value.");
                    } else {
                        continue;
                    }
                } else {
                    if (writeField(obj.get(curFNames[iter1]), curTypes[iter1], fieldOutput)) {
                        recBuilder.addField(iter1, fieldValueBuffer);
                        writeRecord = true;
                    }
                }
            }
        } else {
            //open record type
            IAType curFieldType = null;
            String attrName;
            Iterator<String> iter = obj.fieldNames();
            while(iter.hasNext()) {
                attrName = iter.next();
                if (obj.get(attrName).isNull() || obj.size() == 0) {
                    continue;
                }
                attrIdx = checkAttrNameIdx(curFNames, attrName);
                if (curRecType != null) {
                    curFieldType = curRecType.getFieldType(attrName);
                }
                fieldValueBuffer.reset();
                fieldNameBuffer.reset();
                DataOutput fieldOutput = fieldValueBuffer.getDataOutput();
                if (writeField(obj.get(attrName), curFieldType, fieldOutput)) {
                    writeRecord = true;
                    if (attrIdx == -1) {
                        aString.setValue(attrName);
                        stringSerde.serialize(aString, fieldNameBuffer.getDataOutput());
                        recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                    } else {
                        recBuilder.addField(attrIdx, fieldValueBuffer);
                    }
                }
            }
        }
        if (writeRecord) {
            recBuilder.write(out, true);
        }
        return writeRecord;
    }

    private IARecordBuilder getRecordBuilder() {
        return recordBuilderPool.allocate(ATypeTag.RECORD);
    }

    private IAsterixListBuilder getOrderedListBuilder() {
        return listBuilderPool.allocate(ATypeTag.ORDEREDLIST);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        return (ArrayBackedValueStorage) abvsBuilderPool.allocate(ATypeTag.BINARY);
    }

    @Override
    public void parse(IRawRecord<? extends String> record, DataOutput out) throws HyracksDataException {
        try {
            //TODO get rid of this temporary json
            resetPools();
            ObjectMapper om = new ObjectMapper();
            writeRecord(om.readTree(record.get()), out, recordType);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    private void resetPools() {
        listBuilderPool.reset();
        recordBuilderPool.reset();
        abvsBuilderPool.reset();
    }
}
