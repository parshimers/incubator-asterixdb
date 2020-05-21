package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import it.unimi.dsi.fastutil.chars.CharSet;
import org.apache.asterix.om.types.ATypeTag;
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
        String str = UTF8StringUtil.toString(ptr.getByteArray(),ptr.getStartOffset()+1);
        out.putInt(str.length());
        byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
        out.put(strBytes);
    }
}
