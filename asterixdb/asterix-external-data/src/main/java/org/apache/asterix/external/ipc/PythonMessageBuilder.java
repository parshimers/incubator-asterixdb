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

import static org.apache.hyracks.api.util.JavaSerializationUtils.getSerializationProvider;
import static org.msgpack.core.MessagePack.Code.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.asterix.external.library.msgpack.MessagePacker;

public class PythonMessageBuilder {
    private static final int MAX_BUF_SIZE = 21 * 1024 * 1024; //21MB.
    MessageType type;
    long dataLength;
    ByteBuffer buf;
    String[] initAry = new String[3];

    public PythonMessageBuilder() {
        this.type = null;
        dataLength = -1;
        this.buf = ByteBuffer.allocate(4096);
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void packHeader() {
        MessagePacker.packFixPos(buf, type.getValue());
    }

    //TODO: this is wrong for any multibyte chars
    private int getStringLength(String s) {
        return s.length();
    }

    public void readHead(ByteBuffer buf) {
        byte typ = buf.get();
        type = MessageType.fromByte(typ);
    }

    public void hello() {
        this.type = MessageType.HELO;
        byte[] serAddr = serialize(new InetSocketAddress("127.0.0.1", 1));
        dataLength = serAddr.length + 5;
        packHeader();
        //TODO:make this cleaner
        buf.put(BIN32);
        buf.putInt(serAddr.length);
        buf.put(serAddr);
    }

    public void quit() {
        this.type = MessageType.QUIT;
        dataLength = getStringLength("QUIT");
        packHeader();
        MessagePacker.packFixStr(buf, "QUIT");
    }

    public void init(String module, String clazz, String fn) {
        this.type = MessageType.INIT;
        initAry[0] = module;
        initAry[1] = clazz;
        initAry[2] = fn;
        dataLength = Arrays.stream(initAry).mapToInt(s -> getStringLength(s)).sum() + 2;
        packHeader();
        MessagePacker.packFixArrayHeader(buf, (byte) initAry.length);
        for (String s : initAry) {
            MessagePacker.packStr(buf, s);
        }
    }

    public void call(byte[] args, int lim, int numArgs) {
        if(args.length > buf.capacity()){
            int growTo = PythonResultRouter.closestPow2(args.length);
            if(growTo > MAX_BUF_SIZE){
                //TODO: something more graceful
                throw new IllegalArgumentException("Reached maximum buffer size");
            }
            buf = ByteBuffer.allocate(growTo);
        }
        buf.clear();
        buf.position(0);
        this.type = MessageType.CALL;
        dataLength = 5 + 1 + lim;
        packHeader();
        //TODO: make this switch between fixarray/array16/array32
        buf.put(ARRAY32);
        buf.putInt(numArgs);
        buf.put(args, 0, lim);
    }

    //this is used to send a serialized java inetaddress to the entrypoint so it can send it back
    //to the IPC subsystem, which needs it. don't use this for anything else.
    private byte[] serialize(Object object) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = getSerializationProvider().newObjectOutputStream(baos)) {
            oos.writeObject(object);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return baos.toByteArray();
    }
}
