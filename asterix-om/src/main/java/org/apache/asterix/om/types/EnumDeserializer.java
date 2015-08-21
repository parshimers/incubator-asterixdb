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

import java.util.HashMap;
import java.util.Map;

public class EnumDeserializer<E extends Enum<E> & IEnumSerializer> {

    public static final EnumDeserializer<ATypeTag> ATYPETAGDESERIALIZER = new EnumDeserializer<ATypeTag>(ATypeTag.class);

    private Map<Byte, E> enumvalMap = new HashMap<Byte, E>();

    private EnumDeserializer(Class<E> enumClass) {
        for (E constant : enumClass.getEnumConstants()) {
            enumvalMap.put(constant.serialize(), constant);
        }
    }

    public E deserialize(byte value) {
        return enumvalMap.get(value);
    }

}
