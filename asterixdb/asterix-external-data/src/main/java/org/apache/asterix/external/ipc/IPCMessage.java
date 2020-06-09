package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;
import org.msgpack.core.MessagePack;

public class IPCMessage {
    public static int HEADER_LENGTH_MIN = 7;
    public static int VERSION_HLEN_IDX = 0;
    public static int TYPE_IDX = 1;
    MessageType type;
    int dataLength;
    ByteBuffer buf;
    ByteBuffer otherBuf;

    public IPCMessage() {
        this.type = null;
        dataLength = -1;
        this.buf = ByteBuffer.wrap(new byte[4096]);
        this.otherBuf = ByteBuffer.wrap(new byte[4096]);
    }

    public ByteBuffer toBytes() {
        return buf;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void packHeader() {
        buf.clear();
        byte ver_hlen = PythonIPCProto.VERSION << 4;
        ver_hlen += (byte) (0x0f & HEADER_LENGTH_MIN);
        MessagePacker.packFixPos(buf, ver_hlen);
        MessagePacker.packFixPos(buf, type.getValue());
        MessagePacker.packInt(buf, dataLength);
    }

    public void readFully(ByteBuffer buf) {
        readHead(buf);
        readBody(type);
    }

    //TODO: THIS IS WRONG UNLESS YOU LIVE IN 1972
    private int getStringLength(String s) {
        return s.length();
    }

    public void readHead(ByteBuffer buf) {
        byte ver_hlen = (byte) (buf.get() & MessagePack.Code.POSFIXINT_MASK);
        byte ver = (byte) (ver_hlen << 4);
        if (ver != PythonIPCProto.VERSION) {
            //die
        }
        byte hlen = (byte) (0x0f & ver_hlen);
        //todo: respect hlen
        type = MessageType.fromByte((byte) (buf.get() & MessagePack.Code.POSFIXINT_MASK));
        dataLength = MessageUnpacker.unpackInt(buf);
    }

    public void hello(String lib) {
        this.type = MessageType.HELO;
        dataLength = getStringLength(lib + 1);
        packHeader();
        MessagePacker.packFixStr(buf, lib);
    }

    public void quit(String lib) {
        this.type = MessageType.QUIT;
        dataLength = getStringLength(lib + 1);
        packHeader();
        MessagePacker.packFixStr(buf, lib);
    }

    public void init(String[] ident) {
        this.type = MessageType.INIT;
        dataLength = Arrays.stream(ident).mapToInt(s -> getStringLength(s)).sum() + 2;
        packHeader();
        MessagePacker.packFixArrayHeader(buf, (byte) ident.length);
        for (String s : ident) {
            MessagePacker.packStr(buf, s);
        }
    }

    public void call(String fn, byte[] args, int numArgs) {
        this.type = MessageType.CALL;
        dataLength = getStringLength(fn) + 2 + args.length;
        //FIX THIS - 15 PARAM LIMIT
        MessagePacker.packFixArrayHeader(buf, (byte) (numArgs + 1));
        MessagePacker.packStr(buf, fn);
        buf.put(args);

    }

    private void readBody(MessageType type) {
        switch (type) {
            case CALL_RSP:
                callResp();
                break;
            case INIT_RSP:
                initResp();
                break;
            case HELO:
                heloResp();
                break;
            case QUIT:
                quitResp();
                break;
            default:
        }
    }

    public ByteBuffer callResp() {
        //caller needs to decide how to unpack...?
        otherBuf.reset();
        MessageUnpacker.unpackStr(buf, otherBuf);
        //name of fn is in there... ignore it for now
        return buf.slice();
    }

    public boolean initResp() {
        return true;
    }

    public boolean heloResp() {
        //TODO check response
        return true;
    }

    public boolean quitResp() {
        return true;
    }
}
