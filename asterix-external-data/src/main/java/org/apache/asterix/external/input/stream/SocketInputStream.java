/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.input.stream;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketInputStream extends AInputStream {
    private ServerSocket server;
    private Socket socket;
    private InputStream connectionStream;

    public SocketInputStream(ServerSocket server) throws IOException {
        this.server = server;
        socket = server.accept();
        connectionStream = socket.getInputStream();
    }

    @Override
    public int read() throws IOException {
        int read = connectionStream.read();
        while (read < 0) {
            accept();
            read = connectionStream.read();
        }
        return read;
    }

    @Override
    public boolean skipError() throws Exception {
        accept();
        return true;
    }

    @Override
    public int read(byte b[]) throws IOException {
        int read = connectionStream.read(b, 0, b.length);
        while (read < 0) {
            accept();
            read = connectionStream.read(b, 0, b.length);
        }
        return read;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int read = connectionStream.read(b, off, len);
        while (read < 0) {
            accept();
            read = connectionStream.read(b, off, len);
        }
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        return 0;
    }

    @Override
    public int available() throws IOException {
        return 1;
    }

    @Override
    public void close() throws IOException {
        connectionStream.close();
        socket.close();
        server.close();
    }

    private void accept() throws IOException {
        connectionStream.close();
        socket.close();
        socket = server.accept();
        connectionStream = socket.getInputStream();
    }

    @Override
    public boolean stop() throws Exception {
        return false;
    }
}
