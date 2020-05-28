package org.apache.asterix.external.ipc;

public class IPCMessage {
    public static int HEADER_LENGTH_MIN = 6;
    public static int VERSION_HLEN_IDX = 0;
    public static int TYPE_IDX = 1;
    public static int DATALEN_IDX=2;
    MessageType type;
    int dataLength;
    byte[] data;

    public IPCMessage(){
        this.type = null;
        dataLength = -1;
    }

    public void setType(MessageType type){
        this.type = type;
    }

    public byte[] getHeader(){
        if(dataLength == -1) return null;
        byte[] header = new byte[HEADER_LENGTH_MIN];
        header[HEADER_LENGTH_MIN] = PythonIPCProto.VERSION << 4;
        header[VERSION_HLEN_IDX] += HEADER_LENGTH_MIN;
        header[TYPE_IDX] = type.getValue();
        for(int i=DATALEN_IDX;i<DATALEN_IDX+Integer.BYTES;i++){
            int mask = Integer.SIZE - (Byte.SIZE*(Integer.BYTES-(i-1)));
            header[i] = (byte)(dataLength >>> (mask));
        }
        return header;
    }

    public byte[] getBody(){
        return data;

    }

    public void giveHello(String lib) {

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
