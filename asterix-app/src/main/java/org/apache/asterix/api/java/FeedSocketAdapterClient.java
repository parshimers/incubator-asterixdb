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

package org.apache.asterix.api.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

public class FeedSocketAdapterClient {
    
    private static final int OUTPUT_BUFFER_SIZE = 32* 1024; 
    private String adapterUrl;
    private int port;
    private Socket socket;
    private String sourceFilePath;
    private int recordCount;
    private int maxCount;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(OUTPUT_BUFFER_SIZE);
    private OutputStream out = null;
    
    public FeedSocketAdapterClient(String adapterUrl, int port, String sourceFilePath, int maxCount) {
        this.adapterUrl = adapterUrl;
        this.port = port;
        this.sourceFilePath = sourceFilePath;
        this.maxCount = maxCount;
    }
    
    public void initialize() {
        try {
            socket = new Socket(adapterUrl, port);
        } catch (IOException e) {
            System.err.println("Problem in creating socket against host "+adapterUrl+" on the port "+port);
            e.printStackTrace();
        }
    }
    
    public void finalize() {
        try {
            socket.close();
        } catch (IOException e) {
            System.err.println("Problem in closing socket against host "+adapterUrl+" on the port "+port);
            e.printStackTrace();
        }       
    }

    public void ingest() {
        recordCount = 0;
        BufferedReader br = null;
        try {
            out = socket.getOutputStream();
            br = new BufferedReader(new FileReader(sourceFilePath));
            String nextRecord;
            byte[] b = null;
            byte[] newLineBytes = "\n".getBytes();

            while ((nextRecord = br.readLine()) != null) {
                b = nextRecord.getBytes();
                if (outputBuffer.position() + b.length > outputBuffer.limit() - newLineBytes.length) {
                    outputBuffer.put(newLineBytes);
                    flush();
                    outputBuffer.put(b);
                } else {
                    outputBuffer.put(b);
                }
                recordCount++;
                if (recordCount == maxCount) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private void flush() throws IOException {
        outputBuffer.flip();
        out.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(OUTPUT_BUFFER_SIZE);
    }

}
