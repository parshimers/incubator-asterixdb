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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.library.msgpack.MessagePackUtils;
import org.apache.asterix.external.library.msgpack.MessagePackerFromADM;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.ipc.impl.Message;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

public class PythonIPCProto {

    private final MessagePackerFromADM packerFromADM;
    private PythonMessageBuilder messageBuilder;
    private DataOutputStream sockOut;
    private ByteBuffer headerBuffer = ByteBuffer.allocate(21);
    private ByteBuffer recvBuffer = ByteBuffer.allocate(32768);
    private ExternalFunctionResultRouter router;
    private long routeId;
    private Pair<ByteBuffer, Exception> bufferBox;
    private Process pythonProc;
    private long maxFunctionId;
    private ArrayBufferInput unpackerInput;
    private MessageUnpacker unpacker;
    private ArrayBackedValueStorage argsStorage;

    public PythonIPCProto(OutputStream sockOut, ExternalFunctionResultRouter router, Process pythonProc) {
        this.sockOut = new DataOutputStream(sockOut);
        messageBuilder = new PythonMessageBuilder();
        this.router = router;
        this.pythonProc = pythonProc;
        this.maxFunctionId = 0l;
        unpackerInput = new ArrayBufferInput(new byte[0]);
        unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
        this.packerFromADM = new MessagePackerFromADM();
        this.argsStorage = new ArrayBackedValueStorage();
    }

    public void start() {
        Pair<Long, Pair<ByteBuffer, Exception>> keyAndBufferBox = router.insertRoute(recvBuffer);
        this.routeId = keyAndBufferBox.getFirst();
        this.bufferBox = keyAndBufferBox.getSecond();
    }

    public void helo() throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.buf.clear();
        messageBuilder.buf.position(0);
        messageBuilder.hello();
        sendHeader(routeId, messageBuilder.buf.position());
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.HELO) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected HELO, recieved " + getResponseType().name());
        }
    }

    public long init(String module, String clazz, String fn) throws IOException, AsterixException {
        long functionId = maxFunctionId++;
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.buf.clear();
        messageBuilder.buf.position(0);
        messageBuilder.init(module, clazz, fn);
        sendHeader(functionId, messageBuilder.buf.position());
        sendMsg();
        receiveMsg();
        if (getResponseType() != MessageType.INIT_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected INIT_RSP, recieved " + getResponseType().name());
        }
        return functionId;
    }

    public ByteBuffer call(long functionId, IAType[] argTypes, IValueReference[] argValues, boolean nullCall)
            throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.buf.clear();
        messageBuilder.buf.position(0);
        argsStorage.reset();
        for (int i = 0; i < argTypes.length; i++) {
            packerFromADM.pack(argValues[i], argTypes[i], argsStorage.getDataOutput(), nullCall);
        }
        int len = argsStorage.getLength() + 5;
        sendHeader(functionId, len);
        messageBuilder.call(argValues.length, len);
        /* !!!HACK!!! */
        sendMsg();
        sockOut.write(argsStorage.getByteArray(),argsStorage.getStartOffset(),argsStorage.getLength());
        sockOut.flush();
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected CALL_RSP, recieved " + getResponseType().name());
        }
        return recvBuffer;
    }


    public ByteBuffer callMulti(long key, ArrayBackedValueStorage args, int numTuples)
            throws IOException, AsterixException {
        recvBuffer.clear();
        recvBuffer.position(0);
        recvBuffer.limit(0);
        messageBuilder.buf.clear();
        messageBuilder.buf.position(0);
        //TODO: clarify that this is lengthof(type) + array 16 tag + array16 len + array32 tag for args + array32 len
        int len = args.getLength() + 4;
        sendHeader(key, len);
        messageBuilder.callMulti(0, numTuples);
        sendMsg();
        sockOut.write(args.getByteArray(), args.getStartOffset(), args.getLength());
        sockOut.flush();
        receiveMsg();
        if (getResponseType() != MessageType.CALL_RSP) {
            throw HyracksDataException.create(org.apache.hyracks.api.exceptions.ErrorCode.ILLEGAL_STATE,
                    "Expected CALL_RSP, recieved " + getResponseType().name());
        }
        return recvBuffer;
    }

    //For future use with interpreter reuse between jobs.
    public void quit() throws HyracksDataException {
        messageBuilder.quit();
        router.removeRoute(routeId);
    }

    public void receiveMsg() throws IOException, AsterixException {
        Exception except = null;
        try {
            synchronized (bufferBox) {
                while ((bufferBox.getFirst().limit() == 0 || bufferBox.getSecond() != null) && pythonProc.isAlive()) {
                    bufferBox.wait(100);
                }
            }
            except = router.getAndRemoveException(routeId);
            if (!pythonProc.isAlive()) {
                except = new IOException("Python process exited with code: " + pythonProc.exitValue());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AsterixException(ErrorCode.EXTERNAL_UDF_EXCEPTION, e);
        }
        if (except != null) {
            throw new AsterixException(except);
        }
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

    public void sendHeader(long key, int msgLen) throws IOException {
        headerBuffer.clear();
        headerBuffer.position(0);
        headerBuffer.putInt(HEADER_SIZE + Integer.BYTES + msgLen);
        headerBuffer.putLong(key);
        headerBuffer.putLong(routeId);
        headerBuffer.put(Message.NORMAL);
        sockOut.write(headerBuffer.array(), 0, HEADER_SIZE + Integer.BYTES);
        sockOut.flush();
    }

    public void sendMsg() throws IOException {
        sockOut.write(messageBuilder.buf.array(), 0, messageBuilder.buf.position());
        sockOut.flush();
    }

    public MessageType getResponseType() {
        return messageBuilder.type;
    }

    public long getRouteId() {
        return routeId;
    }

    public DataOutputStream getSockOut() {
        return sockOut;
    }

}
