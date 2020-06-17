package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;

public class IPCMessage {
    /*
        HEADER FORMAT
        All fields are msgpack
        --------------------------------------------------------
        | VERSION & HLEN |     TYPE     |   DATA LENGTH (DLEN)  |
        | 1 nibble each  |    fixpos    |   fixpos to uint32    |
        |    fixpos (1b) |              |                       |
        ---------------------------------------------------------
     */
    public static int HEADER_LENGTH_MIN = 3;
    public static int HEADER_LENGTH_MAX = 11;
    public static int VERSION_HLEN_IDX = 0;
    public static int TYPE_IDX = 1;
    MessageType type;
    long dataLength;
    ByteBuffer buf;
    ByteBuffer otherBuf;
    String[] initAry = new String[3];

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
        //TODO: know dlen beforehand
        buf.clear();
        byte ver_hlen = 0xB;//placeholder
        int headerPos = buf.position();
        MessagePacker.packFixPos(buf, ver_hlen);
        MessagePacker.packFixPos(buf, type.getValue());
        byte dataSizeSize = MessagePacker.minPackPosLong(buf, dataLength);
        int currPos = buf.position();
        buf.position(headerPos);
        buf.put(((byte)((PythonIPCProto.VERSION<<4)+(dataSizeSize+2 & 0xF))));
        buf.position(currPos);
    }

    public void readFully(ByteBuffer buf) {
        readBody(type);
    }

    //TODO: THIS IS WRONG UNLESS YOU LIVE IN 1972
    private int getStringLength(String s) {
        return s.length();
    }

    public void readHead(ByteBuffer buf) {
        byte ver_hlen = buf.get();
        byte ver = (byte) (ver_hlen << 4);
        if (ver != PythonIPCProto.VERSION) {
            //die
        }
        byte hlen = (byte) (0x0f & ver_hlen);
        //todo: respect hlen
        byte typ = buf.get();
        type = MessageType.fromByte(typ);
        dataLength = MessageUnpacker.unpackNextInt(buf);
    }

    public void hello() {
        this.type = MessageType.HELO;
        dataLength = getStringLength("HELO" + 1);
        packHeader();
        MessagePacker.packFixStr(buf, "HELO");
    }

    public void quit(String lib) {
        this.type = MessageType.QUIT;
        dataLength = getStringLength(lib + 1);
        packHeader();
        MessagePacker.packFixStr(buf, lib);
    }

    public void init(String module, String clazz, String fn) {
        this.type = MessageType.INIT;
        initAry[0] = module;
        initAry[1] = clazz;
        initAry[2] = fn;
        dataLength = Arrays.stream(initAry).mapToInt(s -> getStringLength(s)).sum() + 2;
        packHeader();
        MessagePacker.packFixArrayHeader(buf, (byte) initAry.length);
        for (String s : initAry) {
            MessagePacker.packStr(buf, s);
        }
    }

    public void call(String module, String clazz, String fn, byte[] args, int lim, int numArgs) {
        this.type = MessageType.CALL;
        initAry[0] = module;
        initAry[1] = clazz;
        initAry[2] = fn;
        dataLength = Arrays.stream(initAry).mapToInt(s -> getStringLength(s)).sum() + 3 + lim;
        //FIX THIS - 15 PARAM LIMIT
        packHeader();
        MessagePacker.packFixArrayHeader(buf, (byte) initAry.length);
        for (String s : initAry) {
            MessagePacker.packStr(buf, s);
        }
        MessagePacker.packFixArrayHeader(buf, (byte) numArgs);
        buf.put(args,0,lim);
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
