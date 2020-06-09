package org.apache.asterix.external.library.msgpack;

import static org.msgpack.core.MessagePack.Code.*;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.util.encoding.VarLenIntEncoderDecoder;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class MessageUnpacker {

    public static void unpack(ByteBuffer in, ByteBuffer out) {
        byte tag = in.get();
        if (isFixStr(tag)) {
            int len = tag ^ FIXSTR_PREFIX;
            unpackFixStr(in, out);
        } else if (isFixInt(tag)) {
            out.put(ATypeTag.SERIALIZED_INT8_TYPE_TAG);
            if (isPosFixInt(tag)) {
                out.put((byte) (tag ^ POSFIXINT_MASK));
            } else if (isNegFixInt(tag)) {
                out.put((byte) (tag ^ NEGFIXINT_PREFIX));
            }
        } else {
            switch (tag) {
                case INT64:
                    unpackLong(in, out);
                    break;
                case STR32:
                    unpackStr(in, out);
                    break;
                default:
                    throw new IllegalArgumentException("NYI");
            }
        }
        int pos = out.position();
        out.position(0);
        out.limit(pos);
    }

    public static void unpackLong(ByteBuffer in, ByteBuffer out) {
        out.put(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
        out.putLong(in.getLong());
    }

    public static void unpackFixStr(ByteBuffer in, ByteBuffer out) {
        byte len = ((byte) (in.get() & FIXSTR_PREFIX));
        out.put(in.get(len));
    }

    public static int unpackInt(ByteBuffer in) {
        assert in.get() == INT32;
        return in.getInt();
    }

    public static void unpackStr(ByteBuffer in, ByteBuffer out) {
        out.put(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        CharBuffer res = Charset.forName("UTF-8").decode(in.slice());
        int adv = VarLenIntEncoderDecoder.encode(res.length(), in.array(), in.position());
        in.position(in.position() + adv);
        out.put(UTF8StringUtil.writeStringToBytes(res.toString()));
    }

}
