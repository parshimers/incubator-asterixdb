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

public class FloatToInt32TypeConvertComputer implements ITypeConvertComputer {

    public static final FloatToInt32TypeConvertComputer INSTANCE = new FloatToInt32TypeConvertComputer();

    private FloatToInt32TypeConvertComputer() {

    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        float sourceValue = FloatPointable.getFloat(data, start);
        // Boundary check
        if (sourceValue > Integer.MAX_VALUE || sourceValue < Integer.MIN_VALUE) {
            throw new IOException("Cannot convert Float to INT32 - Float value " + sourceValue
                    + " is out of range that INT32 type can hold: INT32.MAX_VALUE:" + Integer.MAX_VALUE
                    + ", INT32.MIN_VALUE: " + Integer.MIN_VALUE);
        }
        // Math.floor to truncate decimal portion
        int targetValue = (int) Math.floor(sourceValue);
        out.writeByte(ATypeTag.INT32.serialize());
        out.writeInt(targetValue);
    }

}
