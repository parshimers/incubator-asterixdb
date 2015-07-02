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

package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class IntermediateAvgAggregateFunction extends AbstractAvgAggregateFunction {

    public IntermediateAvgAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        super(args, output);
    }

    @Override
    public void step(IFrameTupleReference tuple) throws AlgebricksException {
        processPartialResults(tuple);
    }

    @Override
    public void finish() throws AlgebricksException {
        finishPartialResults();
    }

    @Override
    public void finishPartial() throws AlgebricksException {
        finishPartialResults();
    }

    @Override
    protected void processNull() {
        aggType = ATypeTag.NULL;
    }

    @Override
    protected boolean skipStep() {
        return (aggType == ATypeTag.NULL);
    }

}
