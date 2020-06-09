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

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.UUID;

import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.ipc.PythonIPCProto;
import org.apache.asterix.external.library.java.JObjectPointableVisitor;
import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.util.file.FileUtil;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final PointableAllocator pointableAllocator;
    private final JObjectPointableVisitor pointableVisitor;
    private final ByteBuffer argHolder;
    private final ByteBuffer resultWrapper;
    private final ByteBuffer outputWrapper;
    private final IObjectPool<IJObject, IAType> reflectingPool = new ListObjectPool<>(JTypeObjectFactory.INSTANCE);
    private final Map<IAType, TypeInfo> infoPool = new HashMap<>();
    private static final String ENTRYPOINT = "entrypoint.py";
    private static final String PY_NO_SITE_PKGS_OPT = "-S";
    private static final String PY_NO_USER_PKGS_OPT = "-s";

    private final IPointable[] argValues;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);

        File pythonPath = new File(ctx.getServiceContext().getAppConfig().getString(NCConfig.Option.PYTHON_HOME));
        this.pointableAllocator = new PointableAllocator();
        this.pointableVisitor = new JObjectPointableVisitor();

        DataverseName dataverseName = FunctionSignature.getDataverseName(finfo.getFunctionIdentifier());
        try {
            libraryEvaluator = PythonLibraryEvaluator.getInstance(dataverseName, finfo, libraryManager, pythonPath,
                    ctx.getTaskContext());
        } catch (IOException | InterruptedException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        this.argHolder = ByteBuffer.wrap(new byte[Short.MAX_VALUE]);
        this.resultWrapper = ByteBuffer.wrap(new byte[Short.MAX_VALUE]);
        this.outputWrapper = ByteBuffer.wrap(new byte[Short.MAX_VALUE]);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argHolder.clear();
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            try {
                setArgument(i, argValues[i]);
            } catch (IOException e) {
                throw new HyracksDataException("Error evaluating Python UDF", e);
            }
        }
        try {
            byte[] res = libraryEvaluator.callPython(argHolder);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (IOException e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
    }

    private static class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {
        Process p;
        IExternalFunctionInfo finfo;
        ILibraryManager libMgr;
        File pythonHome;
        Channel stdIn;

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, IExternalFunctionInfo finfo,
                ILibraryManager libMgr, File pythonHome) {
            super(jobId, evaluatorId);
            this.finfo = finfo;
            this.libMgr = libMgr;
            this.pythonHome = pythonHome;

        }

        private static void inheritIO(final InputStream src, final PrintStream dest) {
            new Thread(new Runnable() {
                public void run() {
                    Scanner sc = new Scanner(src);
                    while (sc.hasNextLine()) {
                        dest.println(sc.nextLine());
                    }
                }
            }).start();
        }

        public void initialize() throws IOException, InterruptedException {
            PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
            List<String> externalIdents = finfo.getExternalIdentifier();
            PythonLibrary library = (PythonLibrary) libMgr.getLibrary(fnId.dataverseName, fnId.libraryName);
            String wd = library.getFile().getAbsolutePath();
            String packageModule = externalIdents.get(0);
            String clazz = "None";
            String fn;
            if (externalIdents.size() > 2) {
                clazz = externalIdents.get(1);
                fn = externalIdents.get(2);
            } else {
                fn = externalIdents.get(1);
            }
//            String sockName = UUID.randomUUID().toString();
//            String sock = FileUtil.joinPath("/tmp", sockName);
            ProcessBuilder pb = new ProcessBuilder(pythonHome.getAbsolutePath(), PY_NO_SITE_PKGS_OPT,
                    PY_NO_USER_PKGS_OPT, ENTRYPOINT, "/tmp/foo.sock", packageModule, clazz, fn);
            pb.directory(new File(wd));
            //            pb.environment().clear();
            File sockFile = new File("/tmp/foo.sock");
            PythonIPCProto proto = new PythonIPCProto();
            proto.init(sockFile.getAbsolutePath());
//            p = pb.start();
//            inheritIO(p.getInputStream(), System.out);
//            inheritIO(p.getErrorStream(), System.err);
//            stdIn = Channels.newChannel(p.getOutputStream());
            proto.recieveMsg();

        }

        byte[] callPython(ByteBuffer arguments) throws IOException {
            //            Object ret = remoteObj.call("nextTuple", arguments.array(), arguments.position());
            //            return (byte[]) ret;
            return null;
        }

        @Override
        public void deallocate() {
            p.destroyForcibly();
        }

        private static PythonLibraryEvaluator getInstance(DataverseName dataverseName, IExternalFunctionInfo finfo,
                ILibraryManager libMgr, File pythonHome, IHyracksTaskContext ctx)
                throws IOException, InterruptedException {
            PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(dataverseName, finfo.getLibrary());
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, finfo, libMgr,
                        pythonHome);
                evaluator.initialize();
                ctx.registerDeallocatable(evaluator);
                ctx.setStateObject(evaluator);
            }
            return evaluator;
        }

        //        private int getFreeHighPort() throws IOException {
        //            int port;
        //            try (ServerSocket socket = new ServerSocket(0)) {
        //                socket.setReuseAddress(true);
        //                port = socket.getLocalPort();
        //            }
        //            return port;
        //        }

        private void waitForPython() throws IOException, InterruptedException {
            for (int i = 0; i < 100; i++) {
                //                try {
                ////                    remoteObj.call("ping");
                //                    break;
                //                } catch (ConnectException e) {
                //                    sleep(100);
                //                }
            }
        }
    }

    private static final class PythonLibraryEvaluatorId {

        private final DataverseName dataverseName;

        private final String libraryName;

        private PythonLibraryEvaluatorId(DataverseName dataverseName, String libraryName) {
            this.dataverseName = Objects.requireNonNull(dataverseName);
            this.libraryName = Objects.requireNonNull(libraryName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            PythonLibraryEvaluatorId that = (PythonLibraryEvaluatorId) o;
            return dataverseName.equals(that.dataverseName) && libraryName.equals(that.libraryName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataverseName, libraryName);
        }
    }

    private void setArgument(int index, IValueReference valueReference) throws IOException {
        IAType type = argTypes[index];
        ATypeTag tag = type.getTypeTag();
        switch (tag) {
            case ANY:
                TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
                pointy.set(valueReference);
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                MessagePacker.pack(valueReference, rtTypeTag, argHolder);
                break;
            default:
                throw new IllegalArgumentException("NYI");
        }
    }

    private TypeInfo getTypeInfo(IAType type) {
        TypeInfo typeInfo = infoPool.get(type);
        if (typeInfo == null) {
            typeInfo = new TypeInfo(reflectingPool, type, type.getTypeTag());
            infoPool.put(type, typeInfo);
        }
        return typeInfo;
    }

    private void wrap(byte[] in, DataOutput out) throws HyracksDataException {
        outputWrapper.clear();
        resultWrapper.clear();
        resultWrapper.put(in);
        resultWrapper.position(0);
        resultWrapper.limit(in.length);
        MessageUnpacker.unpack(resultWrapper, outputWrapper);
        byte[] outsnip = Arrays.copyOfRange(outputWrapper.array(), outputWrapper.position(), outputWrapper.limit());
        try {
            out.write(outsnip);
        } catch (IOException e) {
            throw new HyracksDataException(e.getMessage());
        }

    }
}
