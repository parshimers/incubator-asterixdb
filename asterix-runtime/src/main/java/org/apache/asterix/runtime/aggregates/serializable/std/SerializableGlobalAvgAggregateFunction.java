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

package org.apache.asterix.runtime.aggregates.serializable.std;

import java.io.DataOutput;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SerializableGlobalAvgAggregateFunction extends AbstractSerializableAvgAggregateFunction {

    public SerializableGlobalAvgAggregateFunction(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        super(args);
    }

    @Override
    public void step(IFrameTupleReference tuple, byte[] state, int start, int len) throws AlgebricksException {
        processPartialResults(tuple, state, start, len);
    }

    @Override
    public void finish(byte[] state, int start, int len, DataOutput result) throws AlgebricksException {
        finishFinalResults(state, start, len, result);
    }

    @Override
    public void finishPartial(byte[] state, int start, int len, DataOutput result) throws AlgebricksException {
        finishPartialResults(state, start, len, result);
    }

    protected void processNull(byte[] state, int start) {
        state[start + AGG_TYPE_OFFSET] = ATypeTag.NULL.serialize();
    }

    @Override
    protected boolean skipStep(byte[] state, int start) {
        ATypeTag aggType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(state[start + AGG_TYPE_OFFSET]);
        return (aggType == ATypeTag.NULL);
    }

}
