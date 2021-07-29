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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;
import static org.msgpack.core.MessagePack.Code.FIXARRAY_PREFIX;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.ipc.PythonMessageBuilder;
import org.apache.asterix.external.library.msgpack.MessagePackUtils;
import org.apache.asterix.external.library.msgpack.MessageUnpackerToADM;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;

class ExternalScalarPythonFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private final PythonLibraryEvaluator libraryEvaluator;

    private final ArrayBackedValueStorage resultBuffer = new ArrayBackedValueStorage();
    private ByteBuffer argHolder;
    private ByteBuffer outputWrapper;
    private final IEvaluatorContext evaluatorContext;

    private final IPointable[] argValues;
    private final SourceLocation sourceLocation;

    private MessageUnpacker unpacker;
    private ArrayBufferInput unpackerInput;
    private MessageUnpackerToADM unpackerToADM;

    private long fnId;

    ExternalScalarPythonFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        try {
            PythonLibraryEvaluatorFactory evaluatorFactory = new PythonLibraryEvaluatorFactory(ctx.getTaskContext());
            this.libraryEvaluator = evaluatorFactory.getEvaluator(finfo, sourceLoc);
            this.fnId = libraryEvaluator.initialize(finfo);
        } catch (IOException | AsterixException e) {
            throw new HyracksDataException("Failed to initialize Python", e);
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < argValues.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        this.argHolder = ByteBuffer.wrap(new byte[ctx.getTaskContext().getInitialFrameSize()]);
        this.outputWrapper = ByteBuffer.wrap(new byte[ctx.getTaskContext().getInitialFrameSize()]);
        this.evaluatorContext = ctx;
        this.sourceLocation = sourceLoc;
        this.unpackerInput = new ArrayBufferInput(new byte[0]);
        this.unpacker = MessagePack.newDefaultUnpacker(unpackerInput);
        this.unpackerToADM = new MessageUnpackerToADM();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        argHolder.clear();
        boolean nullCall = finfo.getNullCall();
        boolean hasNullArg = false;
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            if (!nullCall) {
                byte[] argBytes = argValues[i].getByteArray();
                int argStart = argValues[i].getStartOffset();
                ATypeTag argType = ATYPETAGDESERIALIZER.deserialize(argBytes[argStart]);
                if (argType == ATypeTag.MISSING) {
                    PointableHelper.setMissing(result);
                    return;
                } else if (argType == ATypeTag.NULL) {
                    hasNullArg = true;
                }
            }
            try {
                int argSize = MessagePackUtils.getPackedLen(argValues[i], argTypes[i], nullCall);
                if (argSize > argHolder.capacity() - argHolder.position()) {
                    ByteBuffer newHolder = ByteBuffer.allocate(argHolder.capacity() * 2);
                    newHolder.put(argHolder);
                    argHolder = newHolder;
                }
                libraryEvaluator.setArgument(argTypes[i], argValues[i], argHolder, nullCall);
            } catch (IOException e) {
                throw new HyracksDataException("Error evaluating Python UDF", e);
            }
        }
        if (!nullCall && hasNullArg) {
            PointableHelper.setNull(result);
            return;
        }
        try {
            ByteBuffer res = libraryEvaluator.callPython(fnId, argHolder, argTypes.length);
            resultBuffer.reset();
            wrap(res, resultBuffer.getDataOutput());
        } catch (Exception e) {
            throw new HyracksDataException("Error evaluating Python UDF", e);
        }
        result.set(resultBuffer);
    }

    private void wrap(ByteBuffer resultWrapper, DataOutput out) throws HyracksDataException {
        //TODO: output wrapper needs to grow with result wrapper
        outputWrapper.clear();
        outputWrapper.position(0);
        try {
            if (resultWrapper == null) {
                outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
                return;
            }
            if ((resultWrapper.get() ^ FIXARRAY_PREFIX) != (byte) 2) {
                throw HyracksDataException.create(AsterixException.create(ErrorCode.EXTERNAL_UDF_EXCEPTION,
                        "Returned result missing outer wrapper"));
            }
            int numresults = resultWrapper.get() ^ FIXARRAY_PREFIX;
            if (numresults > 0) {
                //i dont care much for this
                boolean unpacked = false;
                while (!unpacked) {
                    try {
                        unpackerToADM.unpack(resultWrapper, outputWrapper, true);
                        unpacked = true;
                    } catch (BufferOverflowException e) {
                        if (outputWrapper.capacity() * 2 > PythonMessageBuilder.MAX_BUF_SIZE) {
                            outputWrapper = ByteBuffer.allocate(outputWrapper.capacity() * 2);
                        } else {
                            throw HyracksDataException
                                    .create(org.apache.hyracks.api.exceptions.ErrorCode.RECORD_IS_TOO_LARGE);
                        }
                    }
                }
            }
            unpackerInput.reset(resultWrapper.array(), resultWrapper.position() + resultWrapper.arrayOffset(),
                    resultWrapper.remaining());
            unpacker.reset(unpackerInput);
            int numErrors = unpacker.unpackArrayHeader();
            for (int j = 0; j < numErrors; j++) {
                outputWrapper.put(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
                if (evaluatorContext.getWarningCollector().shouldWarn()) {
                    evaluatorContext.getWarningCollector().warn(
                            Warning.of(sourceLocation, ErrorCode.EXTERNAL_UDF_EXCEPTION, unpacker.unpackString()));
                }
            }
            out.write(outputWrapper.array(), 0, outputWrapper.position() + outputWrapper.arrayOffset());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
