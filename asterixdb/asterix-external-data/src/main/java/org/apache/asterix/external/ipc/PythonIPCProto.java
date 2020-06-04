package org.apache.asterix.external.ipc;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import sun.security.util.IOUtils;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

import static org.apache.asterix.external.ipc.IPCMessage.HEADER_LENGTH_MIN;

public class PythonIPCProto {

    public static final byte VERSION = 1;
    private AFUNIXSocket sock;
    IPCMessage send;
    IPCMessage recv;
    SocketChannel chan;
    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[4096]);

    public PythonIPCProto() throws IOException {
        sock = AFUNIXSocket.newInstance();
        send = new IPCMessage();
        recv = new IPCMessage();
    }

    public void init(String s) throws IOException {
        sock.connect(new AFUNIXSocketAddress(File.createTempFile(s,"sock")));
        chan = sock.getChannel();
        send.hello(s);
        chan.write(send.toBytes());
        recieveMsg();
    }

    void recieveMsg() throws IOException {
        int read = 0;
        int reqRead = -1;
        while(!Thread.interrupted() && read < HEADER_LENGTH_MIN) {
            read += chan.read(recvBuffer);
        }
        recv.readHead(recvBuffer);
        reqRead = recv.dataLength;
        while(!Thread.interrupted() && read < reqRead) {
            read += chan.read(recvBuffer);
        }
    }


}
