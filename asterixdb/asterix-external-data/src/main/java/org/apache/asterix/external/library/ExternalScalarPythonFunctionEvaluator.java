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
import java.net.ConnectException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import net.razorvine.pyro.PyroProxy;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.library.ILibraryManager;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.library.java.base.JByte;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JFloat;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JShort;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.external.library.py.PyObjectPointableVisitor;
import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    protected final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final PointableAllocator pointableAllocator;
    private final PyObjectPointableVisitor pointableVisitor;
    private final Object[] argHolder;

    private final IPointable[] argValues;

    public static final BiMap<Class, Class> typeConv =
            new ImmutableBiMap.Builder<Class, Class>().put(HashMap.class, JRecord.class).put(Byte.class, JByte.class)
                    .put(Short.class, JShort.class).put(Integer.class, JInt.class).put(Long.class, JLong.class)
                    .put(Float.class, JFloat.class).put(Double.class, JDouble.class)
                    .put(ArrayList.class, JOrderedList.class).put(String.class, JString.class).build();

    public ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);

        this.pointableAllocator = new PointableAllocator();
        this.pointableVisitor = new PyObjectPointableVisitor();

        DataverseName dataverseName = FunctionSignature.getDataverseName(finfo.getFunctionIdentifier());
        try {
            libraryEvaluator = PythonLibraryEvaluator.getInstance(dataverseName, finfo, libraryManager, ctx.getTaskContext());
        } catch(IOException e){
            throw new HyracksDataException("Failed to initialize Python",e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        this.argHolder = new Object[args.length];
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            try {
                setArgument(i, argValues[i]);
            } catch (IOException |AsterixException e) {
                throw new HyracksDataException("Error evaluating Python UDF",e);
            }
        }
        try {
            Object res = libraryEvaluator.callPython(argHolder);
            wrap(res,resultBuffer.getDataOutput());
        } catch (IOException | IllegalAccessException | InstantiationException e) {
            throw new HyracksDataException("Error evaluating Python UDF",e);
        }
        result.set(resultBuffer.getByteArray(), resultBuffer.getStartOffset(), resultBuffer.getLength());
    }

    private static class PythonLibraryEvaluator extends AbstractStateObject implements IDeallocatable {
        Process p;
        PyroProxy remoteObj;
        IExternalFunctionInfo finfo;
        ILibraryManager libMgr;
        public static final String ENTRYPOINT_SCRIPT_FILENAME = "initialize_entrypoint.sh";
        public static final String SHELL_PATH = "/bin/bash";

        private PythonLibraryEvaluator(JobId jobId, PythonLibraryEvaluatorId evaluatorId, IExternalFunctionInfo finfo, ILibraryManager libMgr) {
            super(jobId, evaluatorId);
            this.finfo = finfo;
            this.libMgr = libMgr;
        }

        public void initialize() throws IOException {
            PythonLibraryEvaluatorId fnId = (PythonLibraryEvaluatorId) id;
            List<String> externalIdents = finfo.getExternalIdentifier();
            String wd = libMgr.getLibraryUrls(fnId.dataverseName,fnId.libraryName)[0].getFile();
            int port = getFreeHighPort();
            String module = externalIdents.get(0);
            String clazz = externalIdents.get(1);
            String fn = externalIdents.get(2);
            ProcessBuilder pb = new ProcessBuilder(SHELL_PATH, wd + File.separator +ENTRYPOINT_SCRIPT_FILENAME, wd,
                    Integer.toString(port), module,clazz,fn);
            pb.inheritIO();
            p = pb.start();
            remoteObj = new PyroProxy("127.0.0.1", port, "nextTuple");
            waitForPython();
        }

        public Object callPython(Object[] arguments) throws IOException {
            return remoteObj.call("nextTuple", arguments);
        }

        @Override
        public void deallocate() {
            remoteObj.close();
            p.destroy();
        }

        private static PythonLibraryEvaluator getInstance(DataverseName dataverseName, IExternalFunctionInfo finfo, ILibraryManager libMgr,
                IHyracksTaskContext ctx) throws IOException {
            PythonLibraryEvaluatorId evaluatorId = new PythonLibraryEvaluatorId(dataverseName, finfo.getLibrary());
            PythonLibraryEvaluator evaluator = (PythonLibraryEvaluator) ctx.getStateObject(evaluatorId);
            if (evaluator == null) {
                evaluator = new PythonLibraryEvaluator(ctx.getJobletContext().getJobId(), evaluatorId, finfo,libMgr);
                evaluator.initialize();
                ctx.registerDeallocatable(evaluator);
                ctx.setStateObject(evaluator);
            }
            return evaluator;
        }

        private int getFreeHighPort() throws IOException {
            int port;
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
            }
            return port;
        }

        private void waitForPython() throws IOException {
            for (int i = 10; i > 0; i++) {
                try {
                    remoteObj.call("ping");
                    break;
                } catch (ConnectException e) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException f) {
                        //doesn't matter
                    }
                }
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

    private void setArgument(int index, IValueReference valueReference) throws IOException, AsterixException {
        IVisitablePointable pointable;
        Object obj = null;
        IAType type = argTypes[index];
        switch (type.getTypeTag()) {
            case OBJECT:
                pointable = pointableAllocator.allocateRecordValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((ARecordVisitablePointable) pointable, type);
                break;
            case ARRAY:
            case MULTISET:
                pointable = pointableAllocator.allocateListValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((AListVisitablePointable) pointable, type);
                break;
            case ANY:
                TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
                pointy.set(valueReference);
                ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
                IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
                switch (rtTypeTag) {
                    case OBJECT:
                        pointable = pointableAllocator.allocateRecordValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((ARecordVisitablePointable) pointable, rtType);
                        break;
                    case ARRAY:
                    case MULTISET:
                        pointable = pointableAllocator.allocateListValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((AListVisitablePointable) pointable, rtType);
                        break;
                    default:
                        pointable = pointableAllocator.allocateFieldValue(rtType);
                        pointable.set(valueReference);
                        obj = pointableVisitor.visit((AFlatValuePointable) pointable, rtType);
                        break;
                }
                break;
            default:
                pointable = pointableAllocator.allocateFieldValue(type);
                pointable.set(valueReference);
                obj = pointableVisitor.visit((AFlatValuePointable) pointable, type);
                break;
        }
        argHolder[index] = obj;
    }

    private void wrap(Object o, DataOutput out) throws IllegalAccessException, InstantiationException, HyracksDataException {
        Class concrete = o.getClass();
        Class asxConv = typeConv.get(concrete);
        IJObject res = (IJObject) asxConv.newInstance();
        res.setValue(o);
        res.serialize(out,true);
    }
}