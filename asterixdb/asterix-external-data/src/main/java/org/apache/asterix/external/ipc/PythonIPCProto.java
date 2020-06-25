/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.ipc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.logging.log4j.Level;
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
    Executor exec;
    Semaphore started;
    ByteBuffer sendBuffer = ByteBuffer.wrap(new byte[1024*1024*10]);
    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[1024*1024*10]);

    public PythonIPCProto(OutputStream sockOut) throws IOException {
//        started = new Semaphore(1);
//        sockServ = AFUNIXServerSocket.newInstance();
        this.sockOut = sockOut;
        send = new IPCMessage();
        recv = new IPCMessage();
    }

    public void start(File s) throws IOException, InterruptedException {

    }

    public void waitForStarted() throws InterruptedException {
//        started.acquire();
    }

    public void helo() throws IOException {
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
    }

    public int init(String module, String clazz, String fn) throws IOException {
        send.init(module, clazz, fn);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
        return recv.initResp();
    }

    public ByteBuffer call(int ipcId, ByteBuffer args, int numArgs) throws IOException {
        send.call(ipcId, args.array(), args.position(), numArgs);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw new IllegalStateException("Illegal reply recieved, expected CALL_RSP");
        }
        return recv.callResp();
    }

    public void quit() throws IOException {
        send.quit();
    }

    private int readAtLeast(long thresh) throws IOException {
        int read = 0;
        while (!Thread.interrupted() && read < thresh) {
            int rd = sockIn.read(recv.buf.array());
            if (rd != -1) {
                read += rd;
            }
        }
        return read;
    }

    public void receiveMsg() throws IOException {
        //TODO: desync???
        recv.buf.clear();
        recv.buf.position(0);
        int read = readAtLeast(1);
        recv.readVerHlen(recv.buf);
        if (read < recv.headerLength) {
            read += readAtLeast((recv.headerLength - 1) - read);
        }
        recv.readHead(recv.buf);
        while (recv.dataLength + recv.headerLength > recv.buf.array().length) {
            recv.doubleBuffer();
        }
        if (read < recv.headerLength + recv.dataLength) {
            readAtLeast((recv.dataLength + recv.headerLength + 1) - read);
        }
    }

    public void sendMsg() throws IOException {
        sockOut.write(send.buf.array(), 0, send.buf.position());
    }

    public MessageType getResponseType() {
        return recv.type;
    }

}
