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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.StorageUtil;

public class PythonDomainSocketProto extends AbstractPythonIPCProto implements IExternalLangIPCProto {
    private final String wd;
    SocketChannel chan;
    private ByteBuffer headerBuffer;

    public PythonDomainSocketProto(OutputStream sockOut, SocketChannel chan, String wd) {
        super(sockOut);
        this.chan = chan;
        this.wd = wd;
        headerBuffer = ByteBuffer.allocate(21);
    }

    @Override
    public void start() {
    }

    @Override
    public void helo() throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.reset();
        messageBuilder.helloDS(wd);
        sendHeader(routeId, messageBuilder.getLength());
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected HELO, recieved " + getResponseType().name());
        }
    }

    @Override
    public void receiveMsg() throws IOException, AsterixException {
        //NO.
        //TODO: handle reading header size and such. this is broken.
        headerBuffer.clear();
        chan.read(headerBuffer);
        headerBuffer.flip();
        int msgSz = headerBuffer.getInt() - 17;
        if(recvBuffer.capacity() < msgSz){
            recvBuffer = ByteBuffer.allocate(((msgSz/32768)+1)*32768);
        }
        recvBuffer.limit(msgSz);
        chan.read(recvBuffer);
        recvBuffer.flip();
        messageBuilder.readHead(recvBuffer);
        if (messageBuilder.type == MessageType.ERROR) {
            unpackerInput.reset(recvBuffer.array(), recvBuffer.position() + recvBuffer.arrayOffset(),
                    recvBuffer.remaining());
            unpacker.reset(unpackerInput);
            throw new AsterixException(unpacker.unpackString());
        }
    }
    @Override
    public void quit() throws HyracksDataException {
        messageBuilder.quit();
    }

}
