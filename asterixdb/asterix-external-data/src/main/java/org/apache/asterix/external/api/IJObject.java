/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.api;

import java.io.DataOutput;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.asterix.external.library.java.base.JByte;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JFloat;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JShort;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

public interface IJObject<T> {

    BiMap<Class, Class> typeConv = new ImmutableBiMap.Builder<Class, Class>().put(HashMap.class, JRecord.class)
            .put(Byte.class, JByte.class).put(Short.class, JShort.class).put(Integer.class, JInt.class)
            .put(Long.class, JLong.class).put(Float.class, JFloat.class).put(Double.class, JDouble.class)
            .put(ArrayList.class, JOrderedList.class).put(String.class, JString.class).build();

    IAType getIAType();

    IAObject getIAObject();

    void setValue(T o);

    T getValue();

    void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException;

    void reset() throws HyracksDataException;

    void setPool(IObjectPool<IJObject, Class> pool);
}
