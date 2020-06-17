package org.apache.asterix.external.library.msgpack;

import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;
import static org.apache.asterix.om.utils.RecordUtil.FULLY_OPEN_RECORD_TYPE;
import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.asterix.om.pointables.nonvisitor.AListPointable;
import org.apache.asterix.om.pointables.nonvisitor.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.transaction.management.service.locking.TypeUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.*;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessagePacker {

    private static final int TAG_SIZE = 1;
    private static final int TYPE_SIZE = 1;
    private static final int LENGTH_SIZE = 4;
    private static final int ITEM_COUNT_SIZE = 4;
    private static final int ITEM_OFFSET_SIZE = 4;

    public static void pack(IValueReference ptr, ATypeTag type, ByteBuffer out) throws HyracksDataException {
        pack(ptr.getByteArray(),ptr.getStartOffset(),type,true,out);
    }

    public static void pack(byte[] ptr, int offs, ATypeTag type, boolean tagged, ByteBuffer out) throws HyracksDataException {
        int relOffs = tagged ? offs+1 : offs;
        switch (type) {
            case STRING:
                packStr(ptr,relOffs, out);
                break;
            case TINYINT:
                packByte(out,BytePointable.getByte(ptr,relOffs));
                break;
            case SMALLINT:
                packShort(out,ShortPointable.getShort(ptr,relOffs));
                break;
            case INTEGER:
                packInt(out,IntegerPointable.getInteger(ptr,relOffs));
                break;
            case BIGINT:
                packLong(out,LongPointable.getLong(ptr,relOffs));
                break;
            case FLOAT:
                packFloat(out,FloatPointable.getFloat(ptr,relOffs));
                break;
            case DOUBLE:
                packDouble(out,DoublePointable.getDouble(ptr,relOffs));
                break;
            case ARRAY:
                packArray(ptr, out);
            case OBJECT:
                packObject(ptr, out);
            default:
                throw new IllegalArgumentException("NYI");
        }
    }

    public static byte minPackPosLong(ByteBuffer out, long in){
        if(in < 127){
            packFixPos(out,(byte)in);
            return 1;
        }
        else if(in < Byte.MAX_VALUE){
            out.put(UINT8);
            packByte(out,(byte)in);
            return 2;
        }
        else if(in < Short.MAX_VALUE){
            out.put(UINT16);
            packShort(out,(byte)in);
            return 3;
        }
        else if(in < Integer.MAX_VALUE){
            out.put(UINT32);
            packInt(out,(byte)in);
            return 5;
        }
        else{
            out.put(UINT64);
            packLong(out,in);
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

    public static void packFloat(ByteBuffer out, float in){
        out.put(FLOAT32);
        out.putFloat(in);
    }

    public static void packDouble(ByteBuffer out, double in){
        out.put(FLOAT64);
        out.putDouble(in);
    }

    public static void packIntRaw(byte[] out, int in, int offset) {
        for (int i = offset; i < offset + Integer.BYTES; i++) {
            int mask = Integer.SIZE - (Byte.SIZE * (Integer.BYTES - i));
            out[i] = (byte) (in >>> (mask));
        }
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
        String str = UTF8StringUtil.toString(in, offs+1);
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private static void packArray(byte in, int offs, ByteBuffer out) throws HyracksDataException {
        out.put(ARRAY32);

        int itemCount = listPtr.getItemCount();
        out.putInt(itemCount);
        for(int i=0;i<itemCount;i++){
            int offs = listPtr.getItemOffset(FULL_OPEN_ORDEREDLIST_TYPE,i);
            pack(ptr,ATypeTag.values()[listPtr.getType()],out);
        }
    }

    private static void packObject(IValueReference ptr, ByteBuffer out){
        ARecordPointable recPtr = ((ARecordPointable)ptr);
        out.put(MAP32);
        //TODO: this is obviously wrong whenever the evaluator does not give an open record
        //      we need to pass the type through when applicable
        int fieldCt = recPtr.getOpenFieldCount(FULLY_OPEN_RECORD_TYPE);
        out.putInt(fieldCt);
        for(int i=0; i< fieldCt; i++){
            int field = recPtr.getOpenFieldValueOffset(FULLY_OPEN_RECORD_TYPE,i);
        }
        for(rec)

    }

    public static void packFixArrayHeader(ByteBuffer buf, byte numObj) {
        buf.put((byte) (FIXARRAY_PREFIX + (0x0F & numObj)));
    }


}
