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
package org.apache.asterix.om.types;

import org.json.JSONException;
import org.json.JSONObject;

import org.apache.asterix.om.base.IAObject;

public class AUnorderedListType extends AbstractCollectionType {

    private static final long serialVersionUID = 1L;

    /**
     * @param itemType
     *            if null, the collection is untyped
     */
    public AUnorderedListType(IAType itemType, String typeName) {
        super(itemType, typeName);
    }

    @Override
    public ATypeTag getTypeTag() {
        return ATypeTag.UNORDEREDLIST;
    }

    @Override
    public String getDisplayName() {
        return "AUnorderedList";
    }

    @Override
    public String toString() {
        return "{{ " + itemType + " }}";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AUnorderedListType) {
            AUnorderedListType type = (AUnorderedListType) obj;
            return this.itemType.equals(type.itemType);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.itemType.hashCode() * 10;
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
        JSONObject type = new JSONObject();
        type.put("type", AUnorderedListType.class.getName());
        type.put("item-type", itemType.toJSON());
        return type;
    }
}
