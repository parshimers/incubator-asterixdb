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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;
import org.apache.asterix.transaction.management.service.locking.TypeUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.Message;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.logging.log4j.Level;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

public class PythonIPCProto {

    public static final byte VERSION = 1;
    public IPCMessage send;
    public IPCMessage recv;
    public Message sendWrap;
    OutputStream sockOut;
    ByteBuffer sendBuffer = ByteBuffer.wrap(new byte[1024*1024*10]);
    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[1024*1024*10]);
    PythonResultRouter router;
    IPCSystem ipcSys;
    Message outMsg;

    public PythonIPCProto(OutputStream sockOut, PythonResultRouter router, IPCSystem ipcSys) throws IOException {
//        started = new Semaphore(1);
//        sockServ = AFUNIXServer        this.
        this.sockOut = sockOut;
        send = new IPCMessage();
        recv = new IPCMessage();
        this.router = router;
        this.ipcSys = ipcSys;
    }


    public void start( Quadruple<Long,Integer,Integer,Integer> id) {
        router.insertRoute(id,recvBuffer);
    }

    public void waitForStarted() throws InterruptedException {
//        started.acquire();
    }

    public void helo(Quadruple<Long,Integer,Integer,Integer> id) throws IOException {
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
    }

    public int init(Quadruple<Long,Integer,Integer,Integer> id, String module, String clazz, String fn) throws Exception {
        send.init(id,module, clazz, fn);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
        return recv.initResp();
    }

    public ByteBuffer call(Quadruple<Long,Integer,Integer,Integer> id, ByteBuffer args, int numArgs) throws Exception {
        send.call(id, args.array(), args.position(), numArgs);
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

    public void receiveMsg() throws IOException {
        try {
            recvBuffer.wait();
        } catch (InterruptedException e) {
            //TODO: not this
            e.printStackTrace();
        }
    }

    public void sendMsg() throws Exception {
        outMsg.setFlag(Message.NORMAL);
        outMsg.setMessageId(-1);
        outMsg.setRequestMessageId(-1);
        outMsg.setPayload(recv.buf);
        outMsg.write(sendBuffer);
    }

    public MessageType getResponseType() {
        return recv.type;
    }

}
