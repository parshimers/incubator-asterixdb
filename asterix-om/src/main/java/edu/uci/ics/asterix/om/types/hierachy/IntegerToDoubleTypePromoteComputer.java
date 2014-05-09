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
package edu.uci.ics.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;

public class IntegerToDoubleTypePromoteComputer implements ITypePromoteComputer {

    public static final IntegerToDoubleTypePromoteComputer INSTANCE = new IntegerToDoubleTypePromoteComputer();

    private IntegerToDoubleTypePromoteComputer() {

    }

    @Override
    public void promote(byte[] data, int start, int length, DataOutput out)
            throws IOException {
        out.writeByte(ATypeTag.DOUBLE.serialize());
        long val = 0L;
        for (int i = 0; i < length; i++) {
            val += ((long) (data[start + i] & 0xff)) << (8 * (length - 1 - i));
        }
        DoubleSerializerDeserializer.INSTANCE.serialize(Double.valueOf(val), out);
    }

}
