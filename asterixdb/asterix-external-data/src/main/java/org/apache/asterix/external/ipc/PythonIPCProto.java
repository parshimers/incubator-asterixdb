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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.Message;

import static java.lang.Thread.sleep;

public class PythonIPCProto {

    public static final byte VERSION = 1;
    public IPCMessage send;
    public IPCMessage recv;
    public Message sendWrap;
    OutputStream sockOut;
    ByteBuffer sendBuffer = ByteBuffer.wrap(new byte[1024 * 1024 * 10]);
    ByteBuffer recvBuffer = ByteBuffer.wrap(new byte[1024 * 1024 * 10]);
    PythonResultRouter router;
    IPCSystem ipcSys;
    Message outMsg;
    IPayloadSerializerDeserializer serde = new PythonResultRouter.NoOpNoSerJustDe();
    IIPCHandle handle;

    public PythonIPCProto(OutputStream sockOut, PythonResultRouter router, IPCSystem ipcSys) throws IOException {
        //        started = new Semaphore(1);
        //        sockServ = AFUNIXServer        this.
        this.sockOut = sockOut;
        send = new IPCMessage();
        recv = new IPCMessage();
        this.router = router;
        this.ipcSys = ipcSys;
        this.outMsg = new Message(null);
    }

    public void start(Quadruple<Long, Integer, Integer, Integer> id) {
        router.insertRoute(id, recvBuffer);
        try {
            sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void waitForStarted() throws InterruptedException {
        //        started.acquire();
    }

    public void helo(Quadruple<Long, Integer, Integer, Integer> id) throws IOException {
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
    }

    public int init(Quadruple<Long, Integer, Integer, Integer> id, String module, String clazz, String fn)
            throws Exception {
        send.init(id, module, clazz, fn);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw new IllegalStateException("Illegal reply recieved, expected INIT_RSP");
        }
        return recv.initResp();
    }

    public ByteBuffer call(Quadruple<Long, Integer, Integer, Integer> id, ByteBuffer args, int numArgs)
            throws Exception {
        send.call(id, args.array(), args.position(), numArgs);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw new IllegalStateException("Illegal reply recieved, expected CALL_RSP");
        }
        byte[] wat = new byte[recvBuffer.limit()-recvBuffer.position()];
        System.arraycopy(recvBuffer.array(),recvBuffer.position(),wat,0,recvBuffer.limit()-recvBuffer.position()-1);
        return ByteBuffer.wrap(wat);
    }

    public void quit() throws IOException {
        send.quit();
    }

    public void receiveMsg() throws IOException {
        try {
            synchronized (recvBuffer) {
                if (recvBuffer.position() == 0){
                    recvBuffer.wait();
                }
            }
        } catch (InterruptedException e) {
            //TODO: not this
            e.printStackTrace();
        }
        recv.readHead(recvBuffer);
    }

    public void sendMsg() throws Exception {
        outMsg.setFlag(Message.NORMAL);
        outMsg.setMessageId(-1);
        outMsg.setRequestMessageId(-1);
        outMsg.setPayload(send.buf);
        outMsg.write(sendBuffer, serde);
        sockOut.write(sendBuffer.array(),0,sendBuffer.position());
        sockOut.flush();
    }

    public MessageType getResponseType() {
        return recv.type;
    }

}
