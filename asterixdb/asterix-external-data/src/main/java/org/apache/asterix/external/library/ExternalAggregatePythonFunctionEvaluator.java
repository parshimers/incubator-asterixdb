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

import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.om.functions.ExternalFunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

class ExternalAggregatePythonFunctionEvaluator extends ExternalAggregateFunctionEvaluator {

    private final String STEP_IDENTIFIER = "step";
    private final String FINISH_IDENTIFIER = "finish";
    //    private final String FINISH_PARTIAL_IDENTIFIER = "finish_partial";
    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private final ByteBuffer argHolder;
    private final ByteBuffer outputWrapper;
    private final IEvaluatorContext evaluatorContext;

    private final IPointable[] argValues;
    private final SourceLocation sourceLocation;

    private MessageUnpacker unpacker;
    private ArrayBufferInput unpackerInput;
    private MessageUnpackerToADM unpackerToADM;

    private final long initFnId;
    private final long stepFnId;
    private final long finishFnId;
    //    private long finishPartialFnId;

    ExternalAggregatePythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        try {
            PythonLibraryEvaluatorFactory evaluatorFactory = new PythonLibraryEvaluatorFactory(ctx.getTaskContext());
            this.libraryEvaluator = evaluatorFactory.getEvaluator(finfo, sourceLoc);
            // try to initialize function ids
            // modify the function info to get the right functions
            String INIT_IDENTIFIER = "init";
            libraryEvaluator.initializeClass(finfo);
            this.initFnId = libraryEvaluator.initialize(addIdentifier(finfo, INIT_IDENTIFIER));
            this.stepFnId = libraryEvaluator.initialize(addIdentifier(finfo, STEP_IDENTIFIER));
            this.finishFnId = libraryEvaluator.initialize(addIdentifier(finfo, FINISH_IDENTIFIER));
            //            this.finishPartialFnId = libraryEvaluator.initialize(addIdentifier(finfo, FINISH_PARTIAL_IDENTIFIER));
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        //TODO: these should be dynamic. this static size picking is a temporary bodge until this works like
        //      v-size frames do or these construction buffers are removed entirely
        int maxArgSz = ExternalDataUtils.getArgBufferSize();
        this.argHolder = ByteBuffer.wrap(new byte[maxArgSz]);
        this.outputWrapper = ByteBuffer.wrap(new byte[maxArgSz]);
        this.evaluatorContext = ctx;
        this.sourceLocation = sourceLoc;
        this.unpackerInput = new ArrayBufferInput(new byte[0]);
        this.unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
        this.unpackerToADM = new MessageUnpackerToADM();
    }

    @Override
    public void init() throws HyracksDataException {
        argHolder.clear();
        boolean nullCall = finfo.getNullCall();
        try {
            libraryEvaluator.callPython(initFnId, new IAType[0], new IValueReference[0], nullCall);
            resultBuffer.reset();
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        argHolder.clear();
        boolean nullCall = finfo.getNullCall();
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
        }
        try {
            ByteBuffer res = libraryEvaluator.callPython(stepFnId, argTypes, argValues, nullCall);
            resultBuffer.reset();
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {
        boolean nullCall = finfo.getNullCall();
        try {
            ByteBuffer res = libraryEvaluator.callPython(finishFnId, new IAType[0], new IValueReference[0], nullCall);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer);
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        // TODO: figure out how to handle finish partial
    }

    private IExternalFunctionInfo addIdentifier(IExternalFunctionInfo finfo, String identifier) {
        // create new function info with different externalIdentifiers
        List<String> newIdentifiers = new ArrayList<>(finfo.getExternalIdentifier());
        newIdentifiers.set(newIdentifiers.size() - 1, newIdentifiers.get(newIdentifiers.size() - 1) + "." + identifier);
        return new ExternalFunctionInfo(finfo.getFunctionIdentifier(), finfo.getKind(), finfo.getParameterTypes(),
                finfo.getReturnType(), finfo.getResultTypeComputer(), finfo.getLanguage(),
                finfo.getLibraryDataverseName(), finfo.getLibraryName(), newIdentifiers, finfo.getResources(),
                finfo.isFunctional(), finfo.getNullCall());
    }

    private void wrap(ByteBuffer resultWrapper, DataOutput out) throws HyracksDataException {
        //TODO: output wrapper needs to grow with result wrapper
        outputWrapper.clear();
        outputWrapper.position(0);
        try {
            if (resultWrapper == null) {
                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                return;
            }
            if ((resultWrapper.get() ^ FIXARRAY_PREFIX) != (byte) 2) {
                throw HyracksDataException
                        .create(AsterixException.create(ErrorCode.EXTERNAL_UDF_PROTO_RETURN_EXCEPTION));
            }
            int numresults = resultWrapper.get() ^ FIXARRAY_PREFIX;
            if (numresults > 0) {
                unpackerToADM.unpack(resultWrapper, out, true);
            }
            unpackerInput.reset(resultWrapper.array(), resultWrapper.position() + resultWrapper.arrayOffset(),
                    resultWrapper.remaining());
            unpacker.reset(unpackerInput);
            int numErrors = unpacker.unpackArrayHeader();
            for (int j = 0; j < numErrors; j++) {
                out.writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                if (evaluatorContext.getWarningCollector().shouldWarn()) {
                    evaluatorContext.getWarningCollector().warn(
                            Warning.of(sourceLocation, ErrorCode.EXTERNAL_UDF_EXCEPTION, unpacker.unpackString()));
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
