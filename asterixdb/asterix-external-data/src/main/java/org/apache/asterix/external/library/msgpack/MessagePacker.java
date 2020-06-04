package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.transaction.management.service.locking.TypeUtil;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.msgpack.core.MessagePack;

public class MessagePacker {

    public static void pack(IValueReference ptr, ATypeTag type, ByteBuffer out) {
        switch (type) {
            case STRING:
                packStr(ptr, out);
                break;
            case BIGINT:
                packLong(out,ptr.getByteArray(),ptr.getStartOffset(),ptr.getLength());
                break;
            case ARRAY:
                packArray(ptr,out);
            default:
                throw new IllegalArgumentException("NYI");
        }

    }

    public static void packLong(ByteBuffer out, byte[] in, int inOffset, int inLen) {
        out.put(INT64);
        out.put(in, inOffset, inLen - 2);
    }

    public static void packByte(ByteBuffer out, byte in) {
        out.put(INT8);
        out.put(in);
    }


    public static void packInt(ByteBuffer out, int in){
        out.put(INT32);
        out.putInt(in);

    }

    public static void packIntRaw(byte[] out, int in, int offset){
        for(int i=offset;i<offset+Integer.BYTES;i++){
            int mask = Integer.SIZE - (Byte.SIZE*(Integer.BYTES-i));
            out[i] = (byte)(in >>> (mask));
        }
    }

    public static void packFixPos(ByteBuffer out, byte in){
        byte mask = (byte)(1 << 7);
        if((in & mask) != 0){
            throw new IllegalArgumentException("fixint7 must be positive");
        }
        out.put(in);
    }

    public static void packFixStr(ByteBuffer buf, String in){
        byte[] strBytes = in.getBytes(Charset.forName("UTF-8"));
        if(strBytes.length > 31){
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

    private static void packStr(IValueReference ptr, ByteBuffer out) {
        out.put(STR32);
        String str = UTF8StringUtil.toString(ptr.getByteArray(), ptr.getStartOffset()+1);
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        out.putInt(strBytes.length);
        out.put(strBytes);
    }

    private static void packArray(IValueReference ptr, ByteBuffer out) {
        byte[] ary = ptr.getByteArray();
        out.put(ARRAY32);
        int headerSz = (Byte.BYTES*2) + (Integer.BYTES*2);
        int dataStartOff = ptr.getStartOffset() + headerSz;
        int szOffs = ptr.getStartOffset() + (Byte.BYTES*2);
        out.putInt(-1);
        int pos = out.position();
    }

    public static void packFixArrayHeader(ByteBuffer buf,byte numObj){
        buf.put((byte) (FIXARRAY_PREFIX + (0x0F  & numObj)));
    }
}
