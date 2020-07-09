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

import static org.apache.hyracks.ipc.impl.Message.HEADER_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Exchanger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.Message;
import org.msgpack.core.MessagePack;

public class PythonIPCProto {

    public PythonMessageBuilder send;
    public PythonMessageBuilder recv;
    OutputStream sockOut;
    ByteBuffer headerBuffer = ByteBuffer.allocate(21);
    ByteBuffer recvBuffer = ByteBuffer.allocate(4096);
    ExternalFunctionResultRouter router;
    IPCSystem ipcSys;
    Message outMsg;
    IPayloadSerializerDeserializer serde = new ExternalFunctionResultRouter.NoOpNoSerJustDe();
    Long key;
    Exchanger<ByteBuffer> routerExch = new Exchanger<>();

    public PythonIPCProto(OutputStream sockOut, ExternalFunctionResultRouter router, IPCSystem ipcSys)
            throws IOException {
        this.sockOut = sockOut;
        send = new PythonMessageBuilder();
        recv = new PythonMessageBuilder();
        this.router = router;
        this.ipcSys = ipcSys;
        this.outMsg = new Message(null);
    }

    public void start() {
        this.key = router.insertRoute(ByteBuffer.allocate(1024 * 1024), routerExch);
    }

    public void helo() throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        send.buf.clear();
        send.buf.position(0);
        send.hello();
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw new IllegalStateException("Illegal reply received, expected HELO");
        }
    }

    public void init(String module, String clazz, String fn) throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        send.buf.clear();
        send.buf.position(0);
        send.init(module, clazz, fn);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw new IllegalStateException("Illegal reply received, expected INIT_RSP");
        }
    }

    public ByteBuffer call(ByteBuffer args, int numArgs) throws Exception {
        recvBuffer.clear();
        recvBuffer.position(0);
        send.buf.clear();
        send.buf.position(0);
        send.call(args.array(), args.position(), numArgs);
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw new IllegalStateException("Illegal reply received, expected CALL_RSP, recvd: " + getResponseType());
        }
        return recvBuffer;
    }

    public void quit() throws IOException {
        send.quit();
        router.removeRoute(key);
    }

    public void receiveMsg() throws IOException, AsterixException {
        try {
            ByteBuffer swap = routerExch.exchange(recvBuffer);
            if (swap == null) {
                Exception e = router.getException(key);
            }
            recvBuffer = swap;
        } catch (InterruptedException e) {
            //TODO: not this
            e.printStackTrace();
        }
        recv.readHead(recvBuffer);
        if (recv.type == MessageType.ERROR) {
            throw new AsterixException(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                    MessagePack.newDefaultUnpacker(recvBuffer).unpackString());
        }
    }

    public void sendMsg() throws IOException {
        headerBuffer.clear();
        headerBuffer.position(0);
        headerBuffer.putInt(HEADER_SIZE + send.buf.position());
        headerBuffer.putLong(-1);
        headerBuffer.putLong(key);
        headerBuffer.put(Message.NORMAL);
        sockOut.write(headerBuffer.array(), 0, HEADER_SIZE + Integer.BYTES);
        sockOut.write(send.buf.array(), 0, send.buf.position());
        sockOut.flush();
    }

    public MessageType getResponseType() {
        return recv.type;
    }

}
