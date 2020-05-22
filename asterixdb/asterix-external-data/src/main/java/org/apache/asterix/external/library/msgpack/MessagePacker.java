package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.transaction.management.service.locking.TypeUtil;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessagePacker {

    public static void pack(IValueReference ptr, ATypeTag type, ByteBuffer out) {
        switch (type) {
            case STRING:
                packStr(ptr, out);
                break;
            case BIGINT:
                packInt(ptr, out);
                break;
            case ARRAY:
                packArray(ptr,out);
            default:
                throw new IllegalArgumentException("NYI");

        }

    }

    private static void packInt(IValueReference ptr, ByteBuffer out) {
        out.put(INT64);
        out.put(ptr.getByteArray(), ptr.getStartOffset() + 1, ptr.getLength() - 2);
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
}
