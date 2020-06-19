/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.ARRAY32;
import static org.msgpack.core.MessagePack.Code.FALSE;
import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;
import static org.msgpack.core.MessagePack.Code.FIXSTR_PREFIX;
import static org.msgpack.core.MessagePack.Code.FLOAT32;
import static org.msgpack.core.MessagePack.Code.FLOAT64;
import static org.msgpack.core.MessagePack.Code.INT16;
import static org.msgpack.core.MessagePack.Code.INT32;
import static org.msgpack.core.MessagePack.Code.INT64;
import static org.msgpack.core.MessagePack.Code.INT8;
import static org.msgpack.core.MessagePack.Code.MAP32;
import static org.msgpack.core.MessagePack.Code.STR32;
import static org.msgpack.core.MessagePack.Code.TRUE;
import static org.msgpack.core.MessagePack.Code.UINT16;
import static org.msgpack.core.MessagePack.Code.UINT32;
import static org.msgpack.core.MessagePack.Code.UINT64;
import static org.msgpack.core.MessagePack.Code.UINT8;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessagePacker {

    private static final int TYPE_TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;

    public static void pack(IValueReference ptr, IAType type, ByteBuffer out) throws HyracksDataException {
        pack(ptr.getByteArray(), ptr.getStartOffset(), type, true, out);
    }

    public static void pack(byte[] ptr, int offs, IAType type, boolean tagged, ByteBuffer out)
            throws HyracksDataException {
        int relOffs = tagged ? offs + 1 : offs;
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case STRING:
                packStr(ptr, relOffs, out);
                break;
            case BOOLEAN:
                if (BooleanPointable.getBoolean(ptr, relOffs)) {
                    out.put(TRUE);
                } else {
                    out.put(FALSE);
                }
                break;
            case TINYINT:
                packByte(out, BytePointable.getByte(ptr, relOffs));
                break;
            case SMALLINT:
                packShort(out, ShortPointable.getShort(ptr, relOffs));
                break;
            case INTEGER:
                packInt(out, IntegerPointable.getInteger(ptr, relOffs));
                break;
            case BIGINT:
                packLong(out, LongPointable.getLong(ptr, relOffs));
                break;
            case FLOAT:
                packFloat(out, FloatPointable.getFloat(ptr, relOffs));
                break;
            case DOUBLE:
                packDouble(out, DoublePointable.getDouble(ptr, relOffs));
                break;
            case ARRAY:
                packArray(ptr, offs, type, out);
                break;
            case OBJECT:
                packObject(ptr, offs, type, out);
                break;
            default:
                throw new IllegalArgumentException("NYI");
        }
    }

    public static byte minPackPosLong(ByteBuffer out, long in) {
        if (in < 127) {
            packFixPos(out, (byte) in);
            return 1;
        } else if (in < Byte.MAX_VALUE) {
            out.put(UINT8);
            out.put((byte) in);
            return 2;
        } else if (in < Short.MAX_VALUE) {
            out.put(UINT16);
            out.putShort((short) in);
            return 3;
        } else if (in < Integer.MAX_VALUE) {
            out.put(UINT32);
            out.putInt((int) in);
            return 5;
        } else {
            out.put(UINT64);
            out.putLong(in);
            return 9;
        }
    }

    public static void packByte(ByteBuffer out, byte in) {
        out.put(INT8);
        out.put(in);
    }

    public static void packShort(ByteBuffer out, short in) {
        out.put(INT16);
        out.putShort(in);
    }

    public static void packInt(ByteBuffer out, int in) {
        out.put(INT32);
        out.putInt(in);

    }

    public static void packLong(ByteBuffer out, long in) {
        out.put(INT64);
        out.putLong(in);
    }

    public static void packFloat(ByteBuffer out, float in) {
        out.put(FLOAT32);
        out.putFloat(in);
    }

    public static void packDouble(ByteBuffer out, double in) {
        out.put(FLOAT64);
        out.putDouble(in);
    }

    public static void packFixPos(ByteBuffer out, byte in) {
        byte mask = (byte) (1 << 7);
        if ((in & mask) != 0) {
            throw new IllegalArgumentException("fixint7 must be positive");
        }
        out.put(in);
    }

    public static void packFixStr(ByteBuffer buf, String in) {
        byte[] strBytes = in.getBytes(Charset.forName("UTF-8"));
        if (strBytes.length > 31) {
            throw new IllegalArgumentException("fixstr cannot be longer than 31");
        }
        buf.put((byte) (FIXSTR_PREFIX + strBytes.length));
        buf.put(strBytes);
    }

    public static void packStr(ByteBuffer out, String in) {
        out.put(STR32);
        byte[] strBytes = in.getBytes(Charset.forName("UTF-8"));
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private static void packStr(byte[] in, int offs, ByteBuffer out) {
        out.put(STR32);
        //TODO: tagged/untagged. closed support is borked so always tagged rn
        String str = UTF8StringUtil.toString(in, offs);
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    public static void packStr(String str, ByteBuffer out) {
        out.put(STR32);
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private static void packArray(byte[] in, int offs, IAType type, ByteBuffer out) throws HyracksDataException {
        //TODO: - could optimize to pack fixarray/array16 for small arrays
        //      - this code is basically a static version of AListPointable, could be deduped
        AbstractCollectionType collType = (AbstractCollectionType) type;
        out.put(ARRAY32);
        int lenOffs = offs + TYPE_TAG_SIZE + TYPE_SIZE;
        int itemCtOffs = LENGTH_SIZE + lenOffs;
        int itemCt = IntegerPointable.getInteger(in, itemCtOffs);
        boolean fixType = NonTaggedFormatUtil.isFixedSizedCollection(type);
        out.putInt(itemCt);
        for (int i = 0; i < itemCt; i++) {
            if (fixType) {
                int itemOffs = itemCtOffs + ITEM_COUNT_SIZE + (i
                        * NonTaggedFormatUtil.getFieldValueLength(in, 0, collType.getItemType().getTypeTag(), false));
                pack(in, itemOffs, collType.getItemType(), false, out);
            } else {
                int itemOffs =
                        offs + IntegerPointable.getInteger(in, itemCtOffs + ITEM_COUNT_SIZE + (i * ITEM_OFFSET_SIZE));
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[BytePointable.getByte(in, itemOffs)];
                pack(in, itemOffs, BuiltinType.fromTypeTag(tag), true, out);
            }
        }
    }

    private static void packObject(byte[] in, int offs, IAType type, ByteBuffer out) throws HyracksDataException {
        ARecordType recType = (ARecordType) type;
        out.put(MAP32);
        int fieldCt = recType.getFieldNames().length + RecordUtils.getOpenFieldCount(in, offs, recType);
        out.putInt(fieldCt);
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            String field = recType.getFieldNames()[i];
            IAType fieldType = RecordUtils.getClosedFieldType(recType, i);
            packStr(field, out);
            pack(in, RecordUtils.getClosedFieldOffset(in, offs, recType, i), fieldType, false, out);
        }
        if (RecordUtils.isExpanded(in, offs, recType)) {
            for (int i = 0; i < RecordUtils.getOpenFieldCount(in, offs, recType); i++) {
                packStr(in, RecordUtils.getOpenFieldNameOffset(in, offs, recType, i), out);
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[RecordUtils.getOpenFieldTag(in, offs, recType, i)];
                pack(in, RecordUtils.getOpenFieldValueOffset(in, offs, recType, i), BuiltinType.fromTypeTag(tag), true,
                        out);
            }
        }

    }

    public static void packFixArrayHeader(ByteBuffer buf, byte numObj) {
        buf.put((byte) (FIXARRAY_PREFIX + (0x0F & numObj)));
    }

    private static class RecordUtils {

        static final int TAG_SIZE = 1;
        static final int RECORD_LENGTH_SIZE = 4;
        static final int EXPANDED_SIZE = 1;
        static final int OPEN_OFFSET_SIZE = 4;
        static final int CLOSED_COUNT_SIZE = 4;
        static final int FIELD_OFFSET_SIZE = 4;
        static final int OPEN_COUNT_SIZE = 4;
        private static final int OPEN_FIELD_HASH_SIZE = 4;
        private static final int OPEN_FIELD_OFFSET_SIZE = 4;
        private static final int OPEN_FIELD_HEADER = OPEN_FIELD_HASH_SIZE + OPEN_FIELD_OFFSET_SIZE;

        private static boolean isOpen(ARecordType recordType) {
            return recordType == null || recordType.isOpen();
        }

        public static final int getSchemeFieldCount(ARecordType recordType) {
            return recordType.getFieldNames().length;
        }

        public static int getLength(byte[] bytes, int start) {
            return IntegerPointable.getInteger(bytes, start + TAG_SIZE);
        }

        public static boolean isExpanded(byte[] bytes, int start, ARecordType recordType) {
            return isOpen(recordType) && BooleanPointable.getBoolean(bytes, start + TAG_SIZE + RECORD_LENGTH_SIZE);
        }

        public static int getOpenPartOffset(int start, ARecordType recordType) {
            return start + TAG_SIZE + RECORD_LENGTH_SIZE + (isOpen(recordType) ? EXPANDED_SIZE : 0);
        }

        public static int getNullBitmapOffset(byte[] bytes, int start, ARecordType recordType) {
            return getOpenPartOffset(start, recordType) + (isExpanded(bytes, start, recordType) ? OPEN_OFFSET_SIZE : 0)
                    + CLOSED_COUNT_SIZE;
        }

        public static int getNullBitmapSize(ARecordType recordType) {
            return RecordUtil.computeNullBitmapSize(recordType);
        }

        public static boolean isClosedFieldNull(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getNullBitmapSize(recordType) > 0
                    && RecordUtil.isNull(bytes[getNullBitmapOffset(bytes, start, recordType) + fieldId / 4], fieldId);
        }

        public static boolean isClosedFieldMissing(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getNullBitmapSize(recordType) > 0 && RecordUtil
                    .isMissing(bytes[getNullBitmapOffset(bytes, start, recordType) + fieldId / 4], fieldId);
        }

        // -----------------------
        // Closed field accessors.
        // -----------------------

        //        public static final void getClosedFieldValue(ARecordType recordType, int fieldId, DataOutput dOut) throws IOException {
        //            if (isClosedFieldNull(recordType, fieldId)) {
        //                dOut.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
        //            } else if (isClosedFieldMissing(recordType, fieldId)) {
        //                dOut.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
        //            } else {
        //                dOut.write(getClosedFieldTag(recordType, fieldId));
        //                dOut.write(bytes, getClosedFieldOffset(recordType, fieldId), getClosedFieldSize(recordType, fieldId));
        //            }
        //        }

        /**
         * This is always untagged
         *
         * @param recordType
         * @param fieldId
         * @param pointable
         * @throws IOException
         */
        public static final void getClosedFieldValue(byte[] bytes, int start, ARecordType recordType, int fieldId,
                IPointable pointable) throws IOException {
            if (isClosedFieldNull(bytes, start, recordType, fieldId)
                    || isClosedFieldMissing(bytes, start, recordType, fieldId)) {
                throw new IllegalStateException("Can't read a null or missing field");
            }
            pointable.set(bytes, getClosedFieldOffset(bytes, start, recordType, fieldId),
                    getClosedFieldSize(bytes, start, recordType, fieldId));
        }

        public static String getClosedFieldName(ARecordType recordType, int fieldId) {
            return recordType.getFieldNames()[fieldId];
        }

        public static final byte getClosedFieldTag(ARecordType recordType, int fieldId) {
            return getClosedFieldType(recordType, fieldId).getTypeTag().serialize();
        }

        public static final IAType getClosedFieldType(ARecordType recordType, int fieldId) {
            IAType aType = recordType.getFieldTypes()[fieldId];
            if (NonTaggedFormatUtil.isOptional(aType)) {
                // optional field: add the embedded non-null type tag
                aType = ((AUnionType) aType).getActualType();
            }
            return aType;
        }

        public static final int getClosedFieldSize(byte[] bytes, int start, ARecordType recordType, int fieldId)
                throws HyracksDataException {
            if (isClosedFieldNull(bytes, start, recordType, fieldId)) {
                return 0;
            }
            return NonTaggedFormatUtil.getFieldValueLength(bytes,
                    getClosedFieldOffset(bytes, start, recordType, fieldId),
                    getClosedFieldType(recordType, fieldId).getTypeTag(), false);
        }

        public static final int getClosedFieldOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            int offset = getNullBitmapOffset(bytes, start, recordType) + getNullBitmapSize(recordType)
                    + fieldId * FIELD_OFFSET_SIZE;
            return start + IntegerPointable.getInteger(bytes, offset);
        }

        // -----------------------
        // Open field count.
        // -----------------------

        public static final int getOpenFieldCount(byte[] bytes, int start, ARecordType recordType) {
            return isExpanded(bytes, start, recordType)
                    ? IntegerPointable.getInteger(bytes, getOpenFieldCountOffset(bytes, start, recordType)) : 0;
        }

        public static int getOpenFieldCountSize(byte[] bytes, int start, ARecordType recordType) {
            return isExpanded(bytes, start, recordType) ? OPEN_COUNT_SIZE : 0;
        }

        public static int getOpenFieldCountOffset(byte[] bytes, int start, ARecordType recordType) {
            return start + IntegerPointable.getInteger(bytes, getOpenPartOffset(start, recordType));
        }

        // -----------------------
        // Open field accessors.
        // -----------------------

        public static final void getOpenFieldValue(byte[] bytes, int start, ARecordType recordType, int fieldId,
                DataOutput dOut) throws IOException {
            dOut.write(bytes, getOpenFieldValueOffset(bytes, start, recordType, fieldId),
                    getOpenFieldValueSize(bytes, start, recordType, fieldId));
        }

        public static final int getOpenFieldValueOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getOpenFieldNameOffset(bytes, start, recordType, fieldId)
                    + getOpenFieldNameSize(bytes, start, recordType, fieldId);
        }

        public static final int getOpenFieldValueSize(byte[] bytes, int start, ARecordType recordType, int fieldId)
                throws HyracksDataException {
            int offset = getOpenFieldValueOffset(bytes, start, recordType, fieldId);
            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(getOpenFieldTag(bytes, start, recordType, fieldId));
            return NonTaggedFormatUtil.getFieldValueLength(bytes, offset, tag, true);
        }

        public static final void getOpenFieldName(byte[] bytes, int start, ARecordType recordType, int fieldId,
                DataOutput dOut) throws IOException {
            dOut.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            dOut.write(bytes, getOpenFieldNameOffset(bytes, start, recordType, fieldId),
                    getOpenFieldNameSize(bytes, start, recordType, fieldId));
        }

        public static final String getOpenFieldName(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            StringBuilder str = new StringBuilder();
            int offset = getOpenFieldNameOffset(bytes, start, recordType, fieldId);
            return UTF8StringUtil.toString(str, bytes, offset).toString();
        }

        public static int getOpenFieldNameSize(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            int utfleng = UTF8StringUtil.getUTFLength(bytes, getOpenFieldNameOffset(bytes, start, recordType, fieldId));
            return utfleng + UTF8StringUtil.getNumBytesToStoreLength(utfleng);
        }

        public static int getOpenFieldNameOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getOpenFieldOffset(bytes, start, recordType, fieldId);
        }

        public static final byte getOpenFieldTag(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return bytes[getOpenFieldValueOffset(bytes, start, recordType, fieldId)];
        }

        public static int getOpenFieldHashOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getOpenFieldCountOffset(bytes, start, recordType) + getOpenFieldCountSize(bytes, start, recordType)
                    + fieldId * OPEN_FIELD_HEADER;
        }

        public static int getOpenFieldOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return start
                    + IntegerPointable.getInteger(bytes, getOpenFieldOffsetOffset(bytes, start, recordType, fieldId));
        }

        public static int getOpenFieldOffsetOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getOpenFieldHashOffset(bytes, start, recordType, fieldId) + OPEN_FIELD_HASH_SIZE;
        }
    }

}
