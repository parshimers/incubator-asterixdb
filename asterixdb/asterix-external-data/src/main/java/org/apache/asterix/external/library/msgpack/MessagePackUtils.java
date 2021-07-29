package org.apache.asterix.external.library.msgpack;

import static org.apache.hyracks.util.string.UTF8StringUtil.getUTFLength;

import java.nio.charset.StandardCharsets;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.om.utils.RecordUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessagePackUtils {

    private static final int TYPE_TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;

    public static int getPackedLen(IValueReference ptr, IAType type, boolean packUnknown) throws HyracksDataException {
        return getPackedLen(ptr.getByteArray(), ptr.getStartOffset(), type, true, packUnknown);
    }

    public static int getPackedLen(byte[] ptr, int offs, IAType type, boolean tagged, boolean packUnknown)
            throws HyracksDataException {
        int relOffs = tagged ? offs + 1 : offs;
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case BOOLEAN:
            case TINYINT:
                return 1 + 1;
            case SMALLINT:
                return Short.BYTES + 1;
            case INTEGER:
            case FLOAT:
                return Float.BYTES + 1;
            case BIGINT:
            case DOUBLE:
                return Long.BYTES + 1;
            case STRING:
                return VarLenIntEncoderDecoder.decode(ptr, relOffs) + 1 + Integer.BYTES;
            case ARRAY:
            case MULTISET:
                return getPackedCollLen(ptr, offs, type);
            case OBJECT:
                return getPackedObjLen(ptr, offs, type);
            case MISSING:
            case NULL:
                if (packUnknown) {
                    return 1 + 1;
                }
                return 0;
            default:
                throw HyracksDataException.create(AsterixException.create(ErrorCode.PARSER_ADM_DATA_PARSER_CAST_ERROR,
                        tag.name(), "to a msgpack"));
        }
    }

    private static int getPackedCollLen(byte[] in, int offs, IAType type) throws HyracksDataException {
        int totalLen = 5; // tag + item count
        AbstractCollectionType collType = (AbstractCollectionType) type;
        int lenOffs = offs + TYPE_TAG_SIZE + TYPE_SIZE;
        int itemCtOffs = LENGTH_SIZE + lenOffs;
        int itemCt = IntegerPointable.getInteger(in, itemCtOffs);
        boolean fixType = NonTaggedFormatUtil.isFixedSizedCollection(type);
        for (int i = 0; i < itemCt; i++) {
            if (fixType) {
                int itemOffs = itemCtOffs + ITEM_COUNT_SIZE + (i
                        * NonTaggedFormatUtil.getFieldValueLength(in, 0, collType.getItemType().getTypeTag(), false));
                totalLen += getPackedLen(in, itemOffs, collType.getItemType(), false, true);
            } else {
                int itemOffs =
                        offs + IntegerPointable.getInteger(in, itemCtOffs + ITEM_COUNT_SIZE + (i * ITEM_OFFSET_SIZE));
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[BytePointable.getByte(in, itemOffs)];
                getPackedLen(in, itemOffs, TypeTagUtil.getBuiltinTypeByTag(tag), true, true);
            }
        }
        return totalLen;
    }

    private static int getPackedObjLen(byte[] in, int offs, IAType type) throws HyracksDataException {
        ARecordType recType = (ARecordType) type;
        int fieldCt = recType.getFieldNames().length + RecordUtils.getOpenFieldCount(in, offs, recType);
        int totalLen = 5;
        for (int i = 0; i < recType.getFieldNames().length; i++) {
            String field = recType.getFieldNames()[i];
            IAType fieldType = RecordUtils.getClosedFieldType(recType, i);
            byte[] strBytes = field.getBytes(StandardCharsets.UTF_8);
            totalLen += strBytes.length;
            totalLen +=
                    getPackedLen(in, RecordUtils.getClosedFieldOffset(in, offs, recType, i), fieldType, false, true);
        }
        if (RecordUtils.isExpanded(in, offs, recType)) {
            for (int i = 0; i < RecordUtils.getOpenFieldCount(in, offs, recType); i++) {
                totalLen +=
                        VarLenIntEncoderDecoder.decode(in, RecordUtils.getOpenFieldNameOffset(in, offs, recType, i));
                ATypeTag tag = ATypeTag.VALUE_TYPE_MAPPING[RecordUtils.getOpenFieldTag(in, offs, recType, i)];
                totalLen += getPackedLen(in, RecordUtils.getOpenFieldValueOffset(in, offs, recType, i),
                        TypeTagUtil.getBuiltinTypeByTag(tag), true, true);
            }
        }
        return totalLen;
    }

    static class RecordUtils {

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

        public static final IAType getClosedFieldType(ARecordType recordType, int fieldId) {
            IAType aType = recordType.getFieldTypes()[fieldId];
            if (NonTaggedFormatUtil.isOptional(aType)) {
                // optional field: add the embedded non-null type tag
                aType = ((AUnionType) aType).getActualType();
            }
            return aType;
        }

        public static final int getClosedFieldOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            int offset = getNullBitmapOffset(bytes, start, recordType) + getNullBitmapSize(recordType)
                    + fieldId * FIELD_OFFSET_SIZE;
            return start + IntegerPointable.getInteger(bytes, offset);
        }

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

        public static final int getOpenFieldValueOffset(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            return getOpenFieldNameOffset(bytes, start, recordType, fieldId)
                    + getOpenFieldNameSize(bytes, start, recordType, fieldId);
        }

        public static int getOpenFieldNameSize(byte[] bytes, int start, ARecordType recordType, int fieldId) {
            int utfleng = getUTFLength(bytes, getOpenFieldNameOffset(bytes, start, recordType, fieldId));
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
