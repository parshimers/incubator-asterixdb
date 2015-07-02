/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.aggregates.std;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.asterix.om.types.hierachy.ITypeConvertComputer;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyAggregateFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractMinMaxAggregateFunction implements ICopyAggregateFunction {
    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage outputVal = new ArrayBackedValueStorage();
    private ArrayBackedValueStorage tempValForCasting = new ArrayBackedValueStorage();
    protected DataOutput out;
    private ICopyEvaluator eval;
    protected ATypeTag aggType;
    private IBinaryComparator cmp;
    private ITypeConvertComputer tpc;
    private final boolean isMin;

    public AbstractMinMaxAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider, boolean isMin)
            throws AlgebricksException {
        out = provider.getDataOutput();
        eval = args[0].createEvaluator(inputVal);
        this.isMin = isMin;
    }

    @Override
    public void init() {
        aggType = ATypeTag.SYSTEM_NULL;
        outputVal.reset();
        tempValForCasting.reset();
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        inputVal.reset();
        eval.evaluate(tuple);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]);
        if (typeTag == ATypeTag.NULL) {
            processNull();
            return;
        } else if (aggType == ATypeTag.NULL) {
            return;
        } else if (aggType == ATypeTag.SYSTEM_NULL) {
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                // Ignore.
                return;
            }
            // First value encountered. Set type, comparator, and initial value.
            aggType = typeTag;
            // Set comparator.
            IBinaryComparatorFactory cmpFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
                    .getBinaryComparatorFactory(aggType, isMin);
            cmp = cmpFactory.createBinaryComparator();
            // Initialize min value.
            outputVal.assign(inputVal);
        } else if (typeTag != ATypeTag.SYSTEM_NULL && !ATypeHierarchy.isCompatible(typeTag, aggType)) {
            throw new AlgebricksException("Unexpected type " + typeTag + " in aggregation input stream. Expected type "
                    + aggType + ".");
        } else {

            // If a system_null is encountered locally, it would be an error; otherwise if it is seen
            // by a global aggregator, it is simple ignored.
            if (typeTag == ATypeTag.SYSTEM_NULL) {
                processSystemNull();
                return;
            }

            if (ATypeHierarchy.canPromote(aggType, typeTag)) {
                tpc = ATypeHierarchy.getTypePromoteComputer(aggType, typeTag);
                aggType = typeTag;
                cmp = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(aggType, isMin)
                        .createBinaryComparator();
                if (tpc != null) {
                    tempValForCasting.reset();
                    try {
                        tpc.convertType(outputVal.getByteArray(), outputVal.getStartOffset() + 1,
                                outputVal.getLength() - 1, tempValForCasting.getDataOutput());
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                    outputVal.reset();
                    outputVal.assign(tempValForCasting);
                }
                try {
                    if (cmp.compare(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength(),
                            outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength()) < 0) {
                        outputVal.assign(inputVal);
                    }
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }

            } else {
                tpc = ATypeHierarchy.getTypePromoteComputer(typeTag, aggType);
                if (tpc != null) {
                    tempValForCasting.reset();
                    try {
                        tpc.convertType(inputVal.getByteArray(), inputVal.getStartOffset() + 1,
                                inputVal.getLength() - 1, tempValForCasting.getDataOutput());
                    } catch (IOException e) {
                        throw new AlgebricksException(e);
                    }
                    try {
                        if (cmp.compare(tempValForCasting.getByteArray(), tempValForCasting.getStartOffset(),
                                tempValForCasting.getLength(), outputVal.getByteArray(), outputVal.getStartOffset(),
                                outputVal.getLength()) < 0) {
                            outputVal.assign(tempValForCasting);
                        }
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                } else {
                    try {
                        if (cmp.compare(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength(),
                                outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength()) < 0) {
                            outputVal.assign(inputVal);
                        }
                    } catch (HyracksDataException e) {
                        throw new AlgebricksException(e);
                    }
                }

            }
        }
    }

    @Override
    public void finish() throws AlgebricksException {
        try {
            switch (aggType) {
                case NULL: {
                    out.writeByte(ATypeTag.NULL.serialize());
                    break;
                }
                case SYSTEM_NULL: {
                    finishSystemNull();
                    break;
                }
                default: {
                    out.write(outputVal.getByteArray(), outputVal.getStartOffset(), outputVal.getLength());
                    break;
                }
            }
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finish();
    }

    protected boolean skipStep() {
        return false;
    }

    protected abstract void processNull();

    protected abstract void processSystemNull() throws AlgebricksException;

    protected abstract void finishSystemNull() throws IOException;
}
