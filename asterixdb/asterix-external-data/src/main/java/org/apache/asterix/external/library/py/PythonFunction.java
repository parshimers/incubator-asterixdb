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
import java.net.ServerSocket;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.PythonFunctionHelper;

import net.razorvine.pyro.PyroProxy;

public class PythonFunction implements IExternalScalarFunction {

    Process p;
    PyroProxy remoteObj;

    @Override
    public void deinitialize() {
        p.destroy();
        remoteObj.close();
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        PythonFunctionHelper pyfh = ((PythonFunctionHelper) functionHelper);
        Object result = remoteObj.call("sentiment", pyfh.getArguments());
        pyfh.setResult(result);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            port = socket.getLocalPort();
        }
        ProcessBuilder pb = new ProcessBuilder("env", "python3", "entrypoint.py", Integer.toString(port));
        p = pb.start();
        boolean poll = true;
        remoteObj = new PyroProxy("127.0.0.1", port, "sentiment");
        while (poll) {
            try {
                remoteObj.call("ping");
                poll = false;
            } catch (ConnectException e) {
                Thread.sleep(100);
            }
        }
    }
}
