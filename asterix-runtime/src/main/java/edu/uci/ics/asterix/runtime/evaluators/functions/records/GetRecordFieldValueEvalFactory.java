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
package edu.uci.ics.asterix.runtime.evaluators.functions.records;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class GetRecordFieldValueEvalFactory implements ICopyEvaluatorFactory {

    private static final long serialVersionUID = 1L;

    private ICopyEvaluatorFactory recordEvalFactory;
    private ICopyEvaluatorFactory fldNameEvalFactory;
    private ARecordType recordType;

    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    public GetRecordFieldValueEvalFactory(ICopyEvaluatorFactory recordEvalFactory,
            ICopyEvaluatorFactory fldNameEvalFactory, ARecordType recordType) {
        this.recordEvalFactory = recordEvalFactory;
        this.fldNameEvalFactory = fldNameEvalFactory;
        this.recordType = recordType;
    }

    @Override
    public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {
        return new ICopyEvaluator() {
            private boolean first = true;

            private ArrayBackedValueStorage outInput0 = new ArrayBackedValueStorage();
            private ArrayBackedValueStorage outInput1 = new ArrayBackedValueStorage();
            private ICopyEvaluator eval0 = recordEvalFactory.createEvaluator(outInput0);
            private ICopyEvaluator eval1 = fldNameEvalFactory.createEvaluator(outInput1);

            int size = 1;
            private DataOutput out = output.getDataOutput();
            private ByteArrayAccessibleOutputStream subRecordTmpStream = new ByteArrayAccessibleOutputStream();
            private ArrayBackedValueStorage[] abvs = new ArrayBackedValueStorage[size];
            private DataOutput[] dos = new DataOutput[size];

            private ByteArrayInputStream stream;
            private DataInput in;
            private AString fieldName;
            private List<String> fieldList = new ArrayList<String>();

            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ANULL);
            @SuppressWarnings("unchecked")
            private ISerializerDeserializer<AString> stringSerDes = AqlSerializerDeserializerProvider.INSTANCE
                    .getSerializerDeserializer(BuiltinType.ASTRING);

            {
                recordType = recordType.deepCopy(recordType);
            }

            @Override
            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                try {
                    outInput1.reset();
                    eval1.evaluate(tuple);

                    byte[] serFldName = outInput1.getByteArray();
                    if (serFldName[0] != SER_STRING_TYPE_TAG) {
                        nullSerde.serialize(ANull.NULL, out);
                        return;
                    }

                    stream = new ByteArrayInputStream(serFldName, 0, serFldName.length);
                    in = new DataInputStream(stream);
                    fieldName = (AString) stringSerDes.deserialize(in);
                    fieldList.clear();
                    fieldList.add(fieldName.getStringValue());

                    if (first) {
                        FieldAccessUtil.init(abvs, dos, fieldList);
                        first = false;
                    } else {
                        FieldAccessUtil.reset(abvs, dos, fieldList);
                    }
                    FieldAccessUtil.evaluate(tuple, out, eval0, abvs, outInput0, subRecordTmpStream, recordType,
                            fieldList);
                } catch (IOException e) {
                    throw new AlgebricksException(e);
                }
            }
        };
    }
}
