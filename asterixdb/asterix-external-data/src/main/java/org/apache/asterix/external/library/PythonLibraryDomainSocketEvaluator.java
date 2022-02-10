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
package org.apache.asterix.external.library;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.external.ipc.PythonDomainSocketProto;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.ipc.impl.IPCSystem;

public class PythonLibraryDomainSocketEvaluator extends AbstractLibrarySocketEvaluator {


    private final ILibraryManager libMgr;
    private File pythonHome;
    private ExternalFunctionResultRouter router;
    private IPCSystem ipcSys;
    private String sitePkgs;
    private List<String> pythonArgs;
    private Map<String, String> pythonEnv;


    SocketChannel chan;

    public PythonLibraryDomainSocketEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, ILibraryManager libMgr,
                                              TaskAttemptId task,
                                              IWarningCollector warningCollector, SourceLocation sourceLoc) {
        super(jobId, evaluatorId, task, warningCollector, sourceLoc);
        this.libMgr = libMgr;
    }

    public void start() throws IOException, AsterixException {
        PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
        PythonLibrary library =
                (PythonLibrary) libMgr.getLibrary(fnId.getLibraryDataverseName(), fnId.getLibraryName());
        String wd = library.getFile().getAbsolutePath();
        //fixme
        Path sockPath = Path.of("/tmp").resolve("test.socket");
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        StandardProtocolFamily fam;
        SocketAddress sockAddr = null;
        try {
            VarHandle sockEnum = lookup.in(StandardProtocolFamily.class).findStaticVarHandle(StandardProtocolFamily.class,"UNIX",StandardProtocolFamily.class);
            Class domainSock = Class.forName("java.net.UnixDomainSocketAddress");
            MethodType unixDomainSockAddrType = MethodType.methodType(domainSock,Path.class);
            MethodHandle unixDomainSockAddr = lookup.findStatic(domainSock,"of",unixDomainSockAddrType);
            MethodType sockChanMethodType = MethodType.methodType(SocketChannel.class, ProtocolFamily.class);
            MethodHandle sockChanOpen = lookup.findStatic(SocketChannel.class,"open",sockChanMethodType);
            sockAddr = ((SocketAddress) unixDomainSockAddr.invoke(sockPath));
            chan = (SocketChannel) sockChanOpen.invoke(sockEnum.get());
        } catch (NoSuchFieldException e) {
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        chan.connect(sockAddr);
        proto = new PythonDomainSocketProto(Channels.newOutputStream(chan),chan, wd);
        proto.start();
        proto.helo();
    }

    @Override
    public void deallocate() {
        //FIX ME
        try {
            proto.quit();
            chan.close();
        } catch (IOException e){
            // FIX THAT TOO!
        }
    }

    static PythonLibraryDomainSocketEvaluator getInstance(IExternalFunctionInfo finfo, ILibraryManager libMgr,
                                                       IHyracksTaskContext ctx, IWarningCollector warningCollector,
                                                       SourceLocation sourceLoc) throws IOException, AsterixException {
        PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(finfo.getLibraryDataverseName(),
                finfo.getLibraryName(), Thread.currentThread());
        PythonLibraryDomainSocketEvaluator evaluator = (PythonLibraryDomainSocketEvaluator) ctx.getStateObject(evaluatorId);
        if (evaluator == null) {
            evaluator = new PythonLibraryDomainSocketEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, libMgr,
                    ctx.getTaskAttemptId(), warningCollector,
                    sourceLoc);
            ctx.getJobletContext().registerDeallocatable(evaluator);
            evaluator.start();
            ctx.setStateObject(evaluator);
        }
        return evaluator;
    }

}
