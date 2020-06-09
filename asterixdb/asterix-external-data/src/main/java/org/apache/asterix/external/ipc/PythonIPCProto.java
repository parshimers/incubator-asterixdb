package org.apache.asterix.external.ipc;

import static org.apache.asterix.external.ipc.IPCMessage.HEADER_LENGTH_MIN;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class PythonIPCProto {

    public static final byte VERSION = 1;
    private AFUNIXServerSocket sockServ;
    private Socket sock;
    IPCMessage send;
    IPCMessage recv;
    SocketChannel chan;
    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[4096]);

    public PythonIPCProto() throws IOException {
        sockServ = AFUNIXServerSocket.newInstance();
        send = new IPCMessage();
        recv = new IPCMessage();
    }

    public void init(String s) throws IOException {
        AFUNIXSocketAddress addr = new AFUNIXSocketAddress(new File("/tmp","foo.sock"));
        sockServ.bind(addr);
        sock = sockServ.accept();
        chan = sock.getChannel();
    }

    public void recieveMsg() throws IOException {
        int read = 0;
        int reqRead = -1;
        while (!Thread.interrupted() && read < HEADER_LENGTH_MIN) {
            read += chan.read(recvBuffer);
        }
        recv.readHead(recvBuffer);
        reqRead = recv.dataLength;
        while (!Thread.interrupted() && read < reqRead) {
            read += chan.read(recvBuffer);
        }
    }

}
