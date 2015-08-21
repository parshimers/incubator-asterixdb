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
package edu.uci.ics.asterix.runtime.evaluators.functions.temporal;

import java.io.DataOutput;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableTime;
import edu.uci.ics.asterix.om.base.ATime;
import edu.uci.ics.asterix.om.base.temporal.GregorianCalendarSystem;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class CurrentTimeDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = AsterixBuiltinFunctions.CURRENT_TIME;

    public final static IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {

        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new CurrentTimeDescriptor();
        }
    };

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.runtime.base.IScalarFunctionDynamicDescriptor#createEvaluatorFactory(edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory[])
     */
    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
                return new ICopyEvaluator() {

                    private DataOutput out = output.getDataOutput();

                    @SuppressWarnings("unchecked")
                    private ISerializerDeserializer<ATime> timeSerde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.ATIME);
                    private AMutableTime aTime = new AMutableTime(0);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        try {
                            int timeChronon = (int) (System.currentTimeMillis() % GregorianCalendarSystem.CHRONON_OF_DAY);
                            aTime.setValue(timeChronon);
                            timeSerde.serialize(aTime, out);
                        } catch (HyracksDataException hex) {
                            throw new AlgebricksException(hex);
                        }
                    }
                };
            }
        };
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.asterix.om.functions.IFunctionDescriptor#getIdentifier()
     */
    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
