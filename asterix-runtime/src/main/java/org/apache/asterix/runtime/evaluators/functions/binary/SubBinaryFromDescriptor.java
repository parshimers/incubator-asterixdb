/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.runtime.evaluators.functions.binary;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SubBinaryFromDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override public IFunctionDescriptor createFunctionDescriptor() {
            return new SubBinaryFromDescriptor();
        }
    };

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SUBBINARY_FROM;
    }

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new SubBinaryFromToDescriptor.AbstractSubBinaryCopyEvaluator(output, args,
                        getIdentifier().getName()) {
                    @Override protected int getSubLength(IFrameTupleReference tuple) throws AlgebricksException {
                        return ByteArrayPointable.MAX_LENGTH;
                    }
                };
            }
        };
    }

}
