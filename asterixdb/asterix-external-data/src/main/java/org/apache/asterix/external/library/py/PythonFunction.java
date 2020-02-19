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
import java.net.URL;
import java.nio.channels.SocketChannel;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.PythonFunctionHelper;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.commons.io.IOUtils;
import py4j.GatewayServer;

public class PythonFunction implements IExternalScalarFunction {

    private static Jep jep;
    private String packageName = "pytestlib";
    Process p;
    GatewayServer server;
    JDouble res;

    @Override
    public void deinitialize() {
        server.shutdown();
        p.destroy();
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        IHello hello = (IHello) server.getPythonServerEntryPoint(new Class[] { IHello.class });
        Double arg = (Double)((PythonFunctionHelper)functionHelper).getArgumentPrim(0);
        res.reset();
        res.setValue(hello.sqrt(arg));
        functionHelper.setResult(res);
    }



    public interface IHello {
    public String sayHello();

    public String sayHello(int i, String s);

    public double sqrt (double s);
}

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        int port;
        try(ServerSocket socket = new ServerSocket(0)){
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }
        ProcessBuilder pb = new ProcessBuilder("env" ,"python3", "entrypoint.py", Integer.toString(port));
        p = pb.start();

        while(true){
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress("localhost",port+1),100);
                s.close();
                break;
            }
            catch(ConnectException e){
            }
        }
        server = new GatewayServer(null,port,port+1,0,0,null);
        server.start();
        res = new JDouble(0);
    }

}
