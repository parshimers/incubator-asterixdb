package org.apache.asterix.external.ipc;

import org.apache.asterix.external.library.msgpack.MessagePacker;

import java.nio.charset.Charset;

public class IPCMessage {
    public static int HEADER_LENGTH_MIN = 3;
    public static int VERSION_HLEN_IDX = 0;
    public static int TYPE_IDX = 1;
    MessageType type;
    int dataLength;
    byte[] data;
    byte[] header = new byte[HEADER_LENGTH_MIN];

    public IPCMessage(){
        this.type = null;
        dataLength = -1;
    }

    public void setType(MessageType type){
        this.type = type;
    }

    public byte[] getHeader(){
        if(dataLength == -1) return null;
        for(int i=0;i<header.length;i++){
            header[i] = 0;
        }
        byte ver_hlen = PythonIPCProto.VERSION << 4;
        ver_hlen += (byte) (0x0f & HEADER_LENGTH_MIN);
        MessagePacker.packFixPos(header,ver_hlen,VERSION_HLEN_IDX);
        MessagePacker.packFixPos(header,type.getValue(),TYPE_IDX);
        return header;
    }

    public byte[] getBody(){
        return data;

    }

    public void giveHello(String lib) {
        this.type = MessageType.HELO;

        data = new byte[lib.getBytes(Charset.forName("UTF-8")).length+1];
        MessagePacker.packFixStr(data,lib,0);
    }


    public void giveQuit(String lib){

    }

    public byte [] giveInit(String[] ident){


    }

    public byte[] giveCall(String fn, byte[] args){

    }

    public void readBytes(byte[] in){

    }
}
