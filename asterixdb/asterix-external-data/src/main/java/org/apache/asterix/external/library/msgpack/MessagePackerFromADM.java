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

import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getClosedFieldOffset;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getClosedFieldType;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getOpenFieldCount;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getOpenFieldNameOffset;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getOpenFieldTag;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.getOpenFieldValueOffset;
import static org.apache.asterix.external.library.msgpack.MessagePackUtils.RecordUtils.isExpanded;
import static org.apache.hyracks.util.string.UTF8StringUtil.charAt;
import static org.apache.hyracks.util.string.UTF8StringUtil.getModifiedUTF8Len;
import static org.apache.hyracks.util.string.UTF8StringUtil.getNumBytesToStoreLength;
import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;
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
import static org.msgpack.core.MessagePack.Code.NIL;
import static org.msgpack.core.MessagePack.Code.STR32;
import static org.msgpack.core.MessagePack.Code.TRUE;

import java.io.DataOutput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class MessagePackerFromADM {

    private static final int TYPE_TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;
    private final CharsetEncoder encoder;
    private final CharBuffer cbuf;
    private final ByteBuffer utfBuf;

    public MessagePackerFromADM() {
        encoder = StandardCharsets.UTF_8.newEncoder();
        cbuf = CharBuffer.allocate(Short.MAX_VALUE);
        utfBuf = ByteBuffer.allocate(Short.MAX_VALUE);
    }

    public ATypeTag pack(IValueReference ptr, IAType type, DataOutput out, boolean packUnknown)
            throws HyracksDataException {
        return pack(ptr.getByteArray(), ptr.getStartOffset(), type, true, packUnknown, out);
    }

    public ATypeTag pack(byte[] ptr, int offs, IAType type, boolean tagged, boolean packUnknown, DataOutput out)
            throws HyracksDataException {
        int relOffs = tagged ? offs + 1 : offs;
        ATypeTag tag = type.getTypeTag();
        try {
            switch (tag) {
                case STRING:
                    packStr(ptr, relOffs, out);
                    break;
                case BOOLEAN:
                    if (BooleanPointable.getBoolean(ptr, relOffs)) {
                        out.writeByte(TRUE);
                    } else {
                        out.writeByte(FALSE);
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
                case MULTISET:
                    packArray(ptr, offs, type, out);
                    break;
                case OBJECT:
                    packObject(ptr, offs, type, out);
                    break;
                case MISSING:
                case NULL:
                    if (packUnknown) {
                        packNull(out);
                        break;
                    } else {
                        return tag;
                    }
                default:
                    throw HyracksDataException.create(AsterixException
                            .create(ErrorCode.PARSER_ADM_DATA_PARSER_CAST_ERROR, tag.name(), "to a msgpack"));
            }
        } catch (IOException e) {
            if (e instanceof HyracksDataException) {
                throw ((HyracksDataException) (e));
            }
            //TODO: should somehow percolate this properly
            else
                throw HyracksDataException.create(AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION));
        }
        return ATypeTag.TYPE;
    }

    public static ATypeTag peekUnknown(IAType type) {
        switch (type.getTypeTag()) {
            case MISSING:
            case NULL:
                return type.getTypeTag();
            default:
                return ATypeTag.TYPE;
        }
    }

    public static void packNull(DataOutput out) throws IOException {
        out.writeByte(NIL);
    }

    public static void packByte(DataOutput out, byte in) throws IOException {
        out.writeByte(INT8);
        out.writeByte(in);
    }

    public static void packShort(DataOutput out, short in) throws IOException {
        out.writeByte(INT16);
        out.writeShort(in);
    }

    public static void packInt(DataOutput out, int in) throws IOException {
        out.writeByte(INT32);
        out.writeInt(in);

    }

    public static void packLong(DataOutput out, long in) throws IOException {
        out.writeByte(INT64);
        out.writeLong(in);
    }

    public static void packFloat(DataOutput out, float in) throws IOException {
        out.writeByte(FLOAT32);
        out.writeFloat(in);
    }

    public static void packDouble(DataOutput out, double in) throws IOException {
        out.writeByte(FLOAT64);
        out.writeDouble(in);
    }

    public static void packFixPos(DataOutput out, byte in) throws IOException {
        byte mask = (byte) (1 << 7);
        if ((in & mask) != 0) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        out.writeByte(in);
    }

    public static void packFixPos(ByteBuffer buf, byte in) throws HyracksDataException {
        byte mask = (byte) (1 << 7);
        if ((in & mask) != 0) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        buf.put(in);
    }

    public static void packFixStr(DataOutput buf, String in) throws IOException {
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        if (strBytes.length > 31) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        buf.writeByte((byte) (FIXSTR_PREFIX + strBytes.length));
        buf.write(strBytes);
    }

    public static void packFixStr(ByteBuffer buf, String in) throws HyracksDataException {
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        if (strBytes.length > 31) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        buf.put((byte) (FIXSTR_PREFIX + strBytes.length));
        buf.put(strBytes);
    }

    public static void packStr(DataOutput out, String in) throws IOException {
        out.writeByte(STR32);
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        out.writeInt(strBytes.length);
        out.write(strBytes);
    }

    public static void packStr(ByteBuffer out, String in) {
        out.put(STR32);
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private void packStr(byte[] in, int offs, DataOutput out) throws IOException {
        //TODO: tagged/untagged. closed support is borked so always tagged rn
        cbuf.clear();
        cbuf.position(0);
        encoder.reset();
        out.writeByte(STR32);
        final int calculatedLength = getUTFLength(in, offs);
        out.writeInt(calculatedLength);
        PrintTools.writeUTF8StringRaw(in,offs,calculatedLength,out);
    }

    private void packArray(byte[] in, int offs, IAType type, DataOutput out) throws IOException {
        //TODO: - could optimize to pack fixarray/array16 for small arrays
        //      - this code is basically a static version of AListPointable, could be deduped
        AbstractCollectionType collType = (AbstractCollectionType) type;
        out.writeByte(ARRAY32);
        int lenOffs = offs + TYPE_TAG_SIZE + TYPE_SIZE;
        int itemCtOffs = LENGTH_SIZE + lenOffs;
        int itemCt = IntegerPointable.getInteger(in, itemCtOffs);
        boolean fixType = NonTaggedFormatUtil.isFixedSizedCollection(type);
        out.writeInt(itemCt);
        for (int i = 0; i < itemCt; i++) {
            if (fixType) {
                int itemOffs = itemCtOffs + ITEM_COUNT_SIZE + (i
                        * NonTaggedFormatUtil.getFieldValueLength(in, 0, collType.getItemType().getTypeTag(), false));
                pack(in, itemOffs, collType.getItemType(), false, true, out);
            } else {
                int itemOffs =
                        offs + IntegerPointable.getInteger(in, itemCtOffs + ITEM_COUNT_SIZE + (i * ITEM_OFFSET_SIZE));
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[BytePointable.getByte(in, itemOffs)];
                pack(in, itemOffs, TypeTagUtil.getBuiltinTypeByTag(tag), true, true, out);
            }
        }
    }

    private void packObject(byte[] in, int offs, IAType type, DataOutput out) throws IOException {
        ARecordType recType = (ARecordType) type;
        out.writeByte(MAP32);
        int fieldCt = recType.getFieldNames().length + getOpenFieldCount(in, offs, recType);
        out.writeInt(fieldCt);
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            String field = recType.getFieldNames()[i];
            IAType fieldType = getClosedFieldType(recType, i);
            packStr(out, field);
            pack(in, getClosedFieldOffset(in, offs, recType, i), fieldType, false, true, out);
        }
        if (isExpanded(in, offs, recType)) {
            for (int i = 0; i < getOpenFieldCount(in, offs, recType); i++) {
                packStr(in, getOpenFieldNameOffset(in, offs, recType, i), out);
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[getOpenFieldTag(in, offs, recType, i)];
                pack(in, getOpenFieldValueOffset(in, offs, recType, i), TypeTagUtil.getBuiltinTypeByTag(tag), true,
                        true, out);
            }
        }

    }

    public static void packFixArrayHeader(ByteBuffer buf, byte numObj) {
        buf.put((byte) (FIXARRAY_PREFIX + (0x0F & numObj)));
    }

}
