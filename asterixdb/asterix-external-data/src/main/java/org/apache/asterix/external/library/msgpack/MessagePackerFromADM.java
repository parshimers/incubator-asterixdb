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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
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

public class MessagePackerFromADM {

    private static final int TYPE_TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;
    private final CharsetEncoder encoder;
    private final CharBuffer cbuf;

    public MessagePackerFromADM() {
        encoder = StandardCharsets.UTF_8.newEncoder();
        cbuf = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    }

    public ATypeTag pack(IValueReference ptr, IAType type, ByteBuffer out, boolean packUnknown)
            throws HyracksDataException {
        return pack(ptr.getByteArray(), ptr.getStartOffset(), type, true, packUnknown, out);
    }

    public ATypeTag pack(byte[] ptr, int offs, IAType type, boolean tagged, boolean packUnknown, ByteBuffer out)
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
                throw HyracksDataException.create(AsterixException.create(ErrorCode.PARSER_ADM_DATA_PARSER_CAST_ERROR,
                        tag.name(), "to a msgpack"));
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

    public static void packNull(ByteBuffer out) {
        out.put(NIL);
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

    public static void packFixPos(ByteBuffer out, byte in) throws HyracksDataException {
        byte mask = (byte) (1 << 7);
        if ((in & mask) != 0) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "fixint7 must be positive");
        }
        out.put(in);
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

    public static void packStr(ByteBuffer out, String in) {
        out.put(STR32);
        byte[] strBytes = in.getBytes(StandardCharsets.UTF_8);
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private void packStr(byte[] in, int offs, ByteBuffer out) {
        //TODO: tagged/untagged. closed support is borked so always tagged rn
        cbuf.clear();
        cbuf.position(0);
        encoder.reset();
        out.put(STR32);
        final int calculatedLength = getUTFLength(in, offs);
        int remainingLen = calculatedLength;
        final int varSzOffset = getNumBytesToStoreLength(calculatedLength);
        int pos = varSzOffset;
        while (remainingLen > 0) {
            char c = charAt(in, pos + offs);
            cbuf.put(c);
            int charLen = getModifiedUTF8Len(c);
            pos += charLen;
            remainingLen -= charLen;
        }
        int sizeStart = out.position();
        out.putInt(-1);
        cbuf.flip();
        int stringStart = out.position();
        encoder.encode(cbuf, out, true);
        encoder.flush(out);
        out.putInt(sizeStart, out.position() - stringStart);
    }

    private void packArray(byte[] in, int offs, IAType type, ByteBuffer out) throws HyracksDataException {
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
                pack(in, itemOffs, collType.getItemType(), false, true, out);
            } else {
                int itemOffs =
                        offs + IntegerPointable.getInteger(in, itemCtOffs + ITEM_COUNT_SIZE + (i * ITEM_OFFSET_SIZE));
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[BytePointable.getByte(in, itemOffs)];
                pack(in, itemOffs, TypeTagUtil.getBuiltinTypeByTag(tag), true, true, out);
            }
        }
    }

    private void packObject(byte[] in, int offs, IAType type, ByteBuffer out) throws HyracksDataException {
        ARecordType recType = (ARecordType) type;
        out.put(MAP32);
        int fieldCt = recType.getFieldNames().length + getOpenFieldCount(in, offs, recType);
        out.putInt(fieldCt);
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
