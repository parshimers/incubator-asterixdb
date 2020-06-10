package org.apache.asterix.external.ipc;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class PythonIPCProto {

    public static final byte VERSION = 1;
    private AFUNIXServerSocket sockServ;
    private Socket sock;
    public IPCMessage send;
    public IPCMessage recv;
    OutputStream sockOut;
    InputStream sockIn;

    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[4096]);

    public PythonIPCProto() throws IOException {
        sockServ = AFUNIXServerSocket.newInstance();
        send = new IPCMessage();
        recv = new IPCMessage();
    }

    public void start(File s ) throws IOException {
        AFUNIXSocketAddress addr = new AFUNIXSocketAddress(s);
        sockServ.bind(addr);
        sock = sockServ.accept();
        sockOut = sock.getOutputStream();
        sockIn = sock.getInputStream();
    }

    public void helo() throws IOException {
        //wait for HELO
        recieveMsg();
        assert getResponseType() == MessageType.HELO;
    }

    public void init(String module, String clazz, String fn) throws IOException {
        send.init(module, clazz, fn);
        sendMsg();
        recieveMsg();
        assert getResponseType() == MessageType.INIT_RSP;
    }

    public ByteBuffer call(String module, String clazz, String fn, ByteBuffer args, int numArgs) throws IOException {
        send.call(module, clazz, fn, args.array(), args.position(), numArgs);
        sendMsg();
        recieveMsg();
        assert getResponseType() == MessageType.CALL_RSP;
        return recv.callResp();
    }

    public void recieveMsg() throws IOException {
        recv.buf.clear();
        recv.buf.position(0);
        int read = 0;
        long reqRead = -1;
        while (!Thread.interrupted() && read < 1) {
            int rd = sockIn.read(recv.buf.array());
            if (rd != -1) {
                read += rd;
            }
        }
        recv.readHead(recv.buf);
        reqRead = recv.dataLength;
        while (!Thread.interrupted() && read < reqRead) {
            read += sockIn.read(recvBuffer.array());
        }
    }

    public void sendMsg() throws IOException {
        sockOut.write(send.buf.array(), 0, send.buf.position());
    }

    public MessageType getResponseType() {
        return recv.type;
    }

}
