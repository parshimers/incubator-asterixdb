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
package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ABinarySerializerDeserializer;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ABinary implements IAObject {

    private static final int HASH_PREFIX = 31;

    protected byte[] bytes;
    protected int start;
    protected int length;

    public ABinary(byte[] byteArray) {
        this.bytes = byteArray;
        this.start = 0;
        this.length = byteArray.length;
    }

    public ABinary(byte[] byteArray, int start, int length) {
        this.bytes = byteArray;
        this.start = start;
        this.length = length;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public int getStart() {
        return start;
    }

    public int getLength() {
        return getByteArrayContentLength() + SIZE_OF_LEADING_LENGTH_FIELD;
    }

    public int getByteArrayContentLength() {
        return ABinarySerializerDeserializer.getLength(bytes, start);
    }

    public static final int SIZE_OF_LEADING_LENGTH_FIELD = ByteArrayPointable.SIZE_OF_LENGTH;

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitABinary(this);
    }

    @Override
    public IAType getType() {
        return BuiltinType.ABINARY;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ABinary)) {
            return false;
        }
        byte[] x = ((ABinary) obj).getBytes();
        int xStart = ((ABinary) obj).getStart();
        int xLength = ((ABinary) obj).getLength();

        if (getLength() != xLength) {
            return false;
        }
        for (int k = 0; k < xLength; k++) {
            if (bytes[start + k] != x[xStart + k]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash() {
        int m = HASH_PREFIX <= getLength() ? HASH_PREFIX : getLength();
        int h = 0;
        for (int i = 0; i < m; i++) {
            h += 31 * h + bytes[start + i];
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int validLength = getByteArrayContentLength();
        int start = getStart() + SIZE_OF_LEADING_LENGTH_FIELD;
        sb.append("ABinary: [ ");
        for (int i = 0; i < validLength; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(bytes[start + i]);
        }
        sb.append(" ]");
        return sb.toString();

    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        int validLength = getByteArrayContentLength();
        int start = getStart() + SIZE_OF_LEADING_LENGTH_FIELD;
        JSONArray byteArray = new JSONArray();
        for (int i = 0; i < validLength; i++) {
            byteArray.put(bytes[start + i]);
        }
        json.put("ABinary", byteArray);

        return json;
    }

}
