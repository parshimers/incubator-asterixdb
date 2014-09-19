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

import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ShortWithoutTypeInfo implements IAObject {

    protected short value;

    public ShortWithoutTypeInfo(short value) {
        super();
        this.value = value;
    }

    public ShortWithoutTypeInfo(byte[] bytes, int offset, int length) {
        value = valueFromBytes(bytes, offset, length);
    }

    public Short getShortValue() {
        return value;
    }

    @Override
    public IAType getType() {
        return BuiltinType.SHORTWITHOUTTYPEINFO;
    }

    @Override
    public String toString() {
        return "ShortWithoutTypeInfo: {" + value + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ShortWithoutTypeInfo)) {
            return false;
        } else {
            return value == (((ShortWithoutTypeInfo) obj).getShortValue());
        }
    }

    @Override
    public int hashCode() {
        return value;
    }

    private static short valueFromBytes(byte[] bytes, int offset, int length) {
        return (short) (((bytes[offset] & 0xff) << 8) + ((bytes[offset + 1] & 0xff)));
    }

    public byte[] toBytes() {
        return new byte[] { (byte) ((value >>> 8) & 0xff), (byte) ((value >>> 0) & 0xff)};
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitShortWithoutTypeInfo(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public JSONObject toJSON() throws JSONException {
        JSONObject json = new JSONObject();

        json.put("ShortWithoutTypeInfo", value);

        return json;
    }
}
