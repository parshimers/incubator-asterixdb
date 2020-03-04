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

import java.io.IOException;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.PythonFunctionHelper;

import net.razorvine.pyro.PyroProxy;

public class PythonFunction implements IExternalScalarFunction {

    Process p;
    PyroProxy remoteObj;

    @Override
    public void deinitialize() {
        remoteObj.close();
        p.destroy();
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        PythonFunctionHelper pyfh = ((PythonFunctionHelper) functionHelper);
        Object result = remoteObj.call("sentiment", pyfh.getArguments());
        pyfh.setResult(result);
    }

    private int getFreeHighPort() throws IOException{
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }
        return port;
    }

    private void waitForPython() throws IOException{
        for(int i=10;i>0;i++){
            try {
                remoteObj.call("ping");
            } catch (ConnectException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException f){
                    //doesn't matter
                }
            }
        }
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        PythonFunctionHelper pyfh = ((PythonFunctionHelper) functionHelper);
        pyfh.getLibraryDeployedPath();
        int port = getFreeHighPort();
        ProcessBuilder pb = new ProcessBuilder("env", "python3", "entrypoint.py", Integer.toString(port));
        pb.environment().put("PYTHONPATH",Arrays.asList(pyfh.getLibraryDeployedPath()).stream().map(u -> u.toString()).collect(Collectors.joining()));
        p = pb.start();
        remoteObj = new PyroProxy("127.0.0.1", port , "sentiment");
        waitForPython();
    }
}
