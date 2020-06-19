package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;

import static org.msgpack.core.MessagePack.Code.isFixInt;

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
    private static final int MAX_BUF_SIZE = 10 * 1024 * 1024; //python msgpack uses this limit by default
    MessageType type;
    byte headerLength;
    long dataLength;
    ByteBuffer buf;
    String[] initAry = new String[3];

    public IPCMessage() {
        this.type = null;
        dataLength = -1;
        this.buf = ByteBuffer.wrap(new byte[4096]);
    }

    public void doubleBuffer(){
        if(buf.array().length >  MAX_BUF_SIZE){
            throw new UnsupportedOperationException("Maximum buffer size reached.");
        }
        byte[] newBuf = new byte[buf.array().length*2];
        System.arraycopy(buf.array(),0,newBuf,0,buf.array().length-1);
        this.buf = ByteBuffer.wrap(newBuf);
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

    //TODO: THIS IS WRONG UNLESS YOU LIVE IN 1972
    private int getStringLength(String s) {
        return s.length();
    }

    public void readVerHlen(ByteBuffer buf){
        byte ver_hlen = buf.get();
        if(!isFixInt(ver_hlen)){
            //die
        }
        byte ver = (byte)( (ver_hlen << 4) >> 4) ;
        // ver is high 3 bytes. byte 0 is the fixed positive integer mask.
        if (ver != PythonIPCProto.VERSION) {
            //die
        }
        headerLength = (byte) (0x0f & ver_hlen);
    }

    public void readHead(ByteBuffer buf) {
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

    public void quit() {
        this.type = MessageType.QUIT;
        dataLength = getStringLength("QUIT");
        packHeader();
        MessagePacker.packFixStr(buf, "QUIT");
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

    public void call(int ipcId, byte[] args, int lim, int numArgs) {
        buf.clear();
        Arrays.fill(buf.array(),buf.arrayOffset(),buf.arrayOffset()+buf.limit(),(byte)0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + lim;
        //FIX THIS - 15 PARAM LIMIT
        packHeader();
        MessagePacker.packInt(buf,ipcId);
        MessagePacker.packFixArrayHeader(buf, (byte) numArgs);
        buf.put(args,0,lim);
    }


    public ByteBuffer callResp() {
        return buf;
    }

    public int initResp() {
        return (int)MessageUnpacker.unpackNextInt(buf);
    }

    public boolean heloResp() {
        //TODO check response
        return true;
    }

    public boolean quitResp() {
        return true;
    }
}
