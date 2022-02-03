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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PythonDomainSocketProto extends AbstractPythonIPCProto implements IExternalLangIPCProto {
    InputStream sockIn;

    public PythonDomainSocketProto(OutputStream sockOut, InputStream sockIn) {
        super(sockOut);
        this.sockIn = sockIn;
    }

    @Override
    public void start() {
    }

    @Override
    public void receiveMsg() throws IOException, AsterixException {
        //NO.
        recvBuffer.put(sockIn.readAllBytes());
        if (bufferBox.getFirst() != recvBuffer) {
            recvBuffer = bufferBox.getFirst();
        }
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
