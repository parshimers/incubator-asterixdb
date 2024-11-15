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
import edu.uci.ics.hyracks.data.std.primitive.FloatPointable;

public class FloatToInt16TypeConvertComputer implements ITypeConvertComputer {

    public static final FloatToInt16TypeConvertComputer INSTANCE = new FloatToInt16TypeConvertComputer();

    private FloatToInt16TypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        float sourceValue = FloatPointable.getFloat(data, start);
        // Boundary check
        if (sourceValue > Short.MAX_VALUE || sourceValue < Short.MIN_VALUE) {
            throw new IOException("Cannot convert Float to INT16 - Float value " + sourceValue
                    + " is out of range that INT16 type can hold: INT16.MAX_VALUE:" + Short.MAX_VALUE
                    + ", INT16.MIN_VALUE: " + Short.MIN_VALUE);
        }
        // Math.floor to truncate decimal portion
        short targetValue = (short) Math.floor(sourceValue);
        out.writeByte(ATypeTag.INT16.serialize());
        out.writeShort(targetValue);
    }

}
