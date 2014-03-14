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

import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class MinMaxAggregateFunction extends AbstractMinMaxAggregateFunction {
    private final boolean isLocalAgg;

    public MinMaxAggregateFunction(ICopyEvaluatorFactory[] args, IDataOutputProvider provider, boolean isMin,
            boolean isLocalAgg) throws AlgebricksException {
        super(args, provider, isMin);
        this.isLocalAgg = isLocalAgg;
    }

    protected void processNull() {
        aggType = ATypeTag.NULL;
    }

    @Override
    protected boolean skipStep() {
        return (aggType == ATypeTag.NULL);
    }

    @Override
    protected void processSystemNull() throws AlgebricksException {
        if (isLocalAgg) {
            throw new AlgebricksException("Type SYSTEM_NULL encountered in local aggregate.");
        }
    }

    @Override
    protected void finishSystemNull() throws IOException {
        // Empty stream. For local agg return system null. For global agg return null.
        if (isLocalAgg) {
            out.writeByte(ATypeTag.SYSTEM_NULL.serialize());
        } else {
            out.writeByte(ATypeTag.NULL.serialize());
        }
    }

}
