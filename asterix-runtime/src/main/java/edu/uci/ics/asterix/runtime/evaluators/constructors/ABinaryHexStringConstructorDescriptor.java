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

package edu.uci.ics.asterix.runtime.evaluators.constructors;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.ByteArrayHexParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

import java.io.DataOutput;
import java.io.IOException;

public class ABinaryHexStringConstructorDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new ABinaryHexStringConstructorDescriptor();
        }
    };

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new ABinaryConstructorEvaluator(output, args[0], ByteArrayHexParserFactory.INSTANCE);
            }
        };
    }

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.BINARY_HEX_CONSTRUCTOR;
    }

    static class ABinaryConstructorEvaluator implements ICopyEvaluator {
        private DataOutput out;
        private ArrayBackedValueStorage outInput;
        private ICopyEvaluator eval;
        private IValueParser byteArrayParser;

        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ANULL);

        public ABinaryConstructorEvaluator(final IDataOutputProvider output, ICopyEvaluatorFactory copyEvaluatorFactory,
                IValueParserFactory valueParserFactory)
                throws AlgebricksException {
            out = output.getDataOutput();
            outInput = new ArrayBackedValueStorage();
            eval = copyEvaluatorFactory.createEvaluator(outInput);
            byteArrayParser = valueParserFactory.createValueParser();
        }

        @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

            try {
                outInput.reset();
                eval.evaluate(tuple);
                byte[] binary = outInput.getByteArray();

                ATypeTag tt = ATypeTag.VALUE_TYPE_MAPPING[binary[0]];
                if (tt == ATypeTag.NULL) {
                    nullSerde.serialize(ANull.NULL, out);
                } else if (tt == ATypeTag.BINARY) {
                    out.write(outInput.getByteArray(), outInput.getStartOffset(), outInput.getLength());
                } else if (tt == ATypeTag.STRING) {
                    String string = new String(outInput.getByteArray(), 3, outInput.getLength() - 3,
                            "UTF-8");
                    char[] buffer = string.toCharArray();
                    out.write(ATypeTag.BINARY.serialize());
                    byteArrayParser.parse(buffer, 0, buffer.length, out);
                } else {
                    throw new AlgebricksException("binary type of " + tt + "haven't implemented yet.");
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
        }
    }

    ;
}
