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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.ipc.PythonIPCProto;
import org.apache.asterix.external.ipc.ExternalFunctionResultRouter;
import org.apache.asterix.external.library.msgpack.MessagePacker;
import org.apache.asterix.external.library.msgpack.MessageUnpacker;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.api.util.ErrorMessageUtil;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.ipc.impl.IPCSystem;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final ByteBuffer argHolder;
    private final ByteBuffer outputWrapper;
    private final IObjectPool<IJObject, IAType> reflectingPool = new ListObjectPool<>(JTypeObjectFactory.INSTANCE);
    private final Map<IAType, TypeInfo> infoPool = new HashMap<>();
    private final IEvaluatorContext evaluatorContext;
    private static final String ENTRYPOINT = "entrypoint.py";
    private static final String PY_NO_SITE_PKGS_OPT = "-S";
    private static final String PY_NO_USER_PKGS_OPT = "-s";

    private final IPointable[] argValues;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);

        File pythonPath = new File(ctx.getServiceContext().getAppConfig().getString(NCConfig.Option.PYTHON_HOME));
        DataverseName dataverseName = FunctionSignature.getDataverseName(finfo.getFunctionIdentifier());
        try {
            libraryEvaluator = PythonLibraryEvaluator.getInstance(dataverseName, finfo, libraryManager, router, ipcSys,
                    pythonPath, ctx.getTaskContext(), ctx.getWarningCollector());
        } catch (IOException | InterruptedException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        //TODO: these should be dynamic
        this.argHolder = ByteBuffer.wrap(new byte[Short.MAX_VALUE]);
        this.outputWrapper = ByteBuffer.wrap(new byte[Short.MAX_VALUE]);
        this.evaluatorContext = ctx;
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
            ByteBuffer res = libraryEvaluator.callPython(argHolder, argTypes.length);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
    }

    private static class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {
        Process p;
        IExternalFunctionInfo finfo;
        ILibraryManager libMgr;
        File pythonHome;
        PythonIPCProto proto;
        ExternalFunctionResultRouter router;
        IPCSystem ipcSys;
        String module;
        String clazz;
        String fn;
        TaskAttemptId task;
        IWarningCollector warningCollector;

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, IExternalFunctionInfo finfo,
                                       ILibraryManager libMgr, File pythonHome, ExternalFunctionResultRouter router, IPCSystem ipcSys,
                                       TaskAttemptId task, IWarningCollector warningCollector) {
            super(jobId, evaluatorId);
            this.finfo = finfo;
            this.libMgr = libMgr;
            this.pythonHome = pythonHome;
            this.router = router;
            this.task = task;
            this.ipcSys = ipcSys;
            this.warningCollector = warningCollector;

        }

        public void initialize() throws IOException, AsterixException {
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
            this.fn = fn;
            this.clazz = clazz;
            this.module = packageModule;
            int port = ipcSys.getSocketAddress().getPort();
            ProcessBuilder pb = new ProcessBuilder(pythonHome.getAbsolutePath(), PY_NO_SITE_PKGS_OPT,
                    PY_NO_USER_PKGS_OPT, ENTRYPOINT, Integer.toString(port));
            pb.directory(new File(wd));
            pb.environment().clear();
            p = pb.start();
            proto = new PythonIPCProto(p.getOutputStream(), router, ipcSys);
            proto.start();
            try {
                proto.helo();
                proto.init(packageModule, clazz, fn);
            } catch (Exception e) {
                throw AsterixException.create(ErrorCode.LOCAL_NETWORK_ERROR,e);
            }
        }

        ByteBuffer callPython(ByteBuffer arguments, int numArgs) throws Exception {
            return proto.call(arguments, numArgs);

        }

        @Override
        public void deallocate() {
            boolean dead = false;
            try {
                p.destroy();
                dead = p.waitFor(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //gonna kill it anyway
            }
            if (!dead) {
                p.destroyForcibly();
            }
        }

        private static PythonLibraryEvaluator getInstance(DataverseName dataverseName, IExternalFunctionInfo finfo,
                                                          ILibraryManager libMgr, ExternalFunctionResultRouter router, IPCSystem ipcSys, File pythonHome,
                                                          IHyracksTaskContext ctx, IWarningCollector warningCollector) throws IOException, InterruptedException {
            PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(dataverseName, finfo.getLibrary());
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, finfo, libMgr,
                        pythonHome, router, ipcSys, ctx.getTaskAttemptId(), warningCollector);
                evaluator.initialize();
                ctx.registerDeallocatable(evaluator);
                ctx.setStateObject(evaluator);
            }
            return evaluator;
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
                MessagePacker.pack(valueReference, rtType, argHolder);
                break;
            default:
                throw new IllegalArgumentException("NYI");
        }
    }

    private void wrap(ByteBuffer resultWrapper, DataOutput out) throws HyracksDataException {
        //TODO: output wrapper needs to grow with result wrapper
        outputWrapper.clear();
        outputWrapper.position(0);
        MessageUnpacker.unpack(resultWrapper, outputWrapper, true);
        try {
            out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
        } catch (IOException e) {
            throw new HyracksDataException(e.getMessage());
        }

    }
}
