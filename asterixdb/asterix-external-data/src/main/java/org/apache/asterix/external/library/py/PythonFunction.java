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
package org.apache.asterix.external.library.py;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.google.inject.internal.util.$AbstractMapEntry;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.PythonFunctionHelper;
import org.apache.asterix.external.library.java.base.JDouble;

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import py4j.GatewayServer;

public class PythonFunction implements IExternalScalarFunction {

    Process p;
    ServerSocket dataSocket;
    GatewayServer server;
    JDouble res;
    private int tidx = 0;
    PythonFunctionHelper fh;

    @Override
    public void deinitialize() {
        server.shutdown();
        p.destroy();
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        IPythonFunction hello =
                (IPythonFunction) server.getPythonServerEntryPoint(new Class[] { IPythonFunction.class });
        Object[] args = ((PythonFunctionHelper) functionHelper).getArguments();
        Object result = hello.nextTuple(args);

        res.reset();
        ((PythonFunctionHelper) functionHelper).setResult(result);


    }

    @Override
    public void nextFrame(ByteBuffer buf, FrameTupleReference tupleRef, IFrameTupleAccessor accessor){
        int nTuple = accessor.getTupleCount();
        Object[] argList = new Object[nTuple];
        tidx = accessor.getTupleCount();
        if(tidx > 1){
            for(;tidx < nTuple-1;tidx++){
                tupleRef.reset(accessor,tidx);
                for(int i =0;i<fh.argumentEvaluators.)
            }
        }
    }

    public interface IPythonFunction {
        Object nextTuple(Object[] args);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        this.fh = (PythonFunctionHelper)functionHelper;
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }
        try(ServerSocket sock = new ServerSocket(port+2)){
           dataSocket = sock;
        }
        ProcessBuilder pb = new ProcessBuilder("env", "python3", "entrypoint.py", Integer.toString(port));
        p = pb.start();

        while (true) {
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress("localhost", port + 1), 100);
                s.close();
                break;
            } catch (ConnectException e) {
            }
        }

        server = new GatewayServer(null, port, port + 1, 0, 0, null);
        server.start();
        res = new JDouble(0);
    }

}
