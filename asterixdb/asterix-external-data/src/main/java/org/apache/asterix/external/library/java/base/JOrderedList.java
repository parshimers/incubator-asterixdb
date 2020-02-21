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
package org.apache.asterix.external.library.java.base;

import java.io.DataOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.external.api.IJType;
import org.apache.asterix.external.library.PythonFunctionHelper;
import org.apache.asterix.om.base.AMutableOrderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public final class JOrderedList extends JList {

    private AOrderedListType listType;

    public JOrderedList() {
        this(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
    }

    public JOrderedList(IJType listItemType) {
        super();
        this.listType = new AOrderedListType(listItemType.getIAType(), null);
    }

    public JOrderedList(IAType listItemType) {
        super();
        this.listType = new AOrderedListType(listItemType, null);
    }

    public List<IJObject> getValue() {
        return jObjects;
    }

    @Override
    public IAType getIAType() {
        return listType;
    }

    @Override
    public IAObject getIAObject() {
        AMutableOrderedList v = new AMutableOrderedList(listType);
        for (IJObject jObj : jObjects) {
            v.add(jObj.getIAObject());
        }
        return v;
    }

    @Override
    public void setValue(Object o) {
        List<Object> vals = (ArrayList) o;
        if (vals.size() > 0) {
            Object first = vals.get(0);
            Class asxClass = PythonFunctionHelper.typeConv.get(first.getClass());
            Constructor cs = null;
            try {
                cs = asxClass.getConstructor();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
            IJObject obj = null;
            try {
                obj = (IJObject) cs.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            obj.setValue(first);
            IAType listType = obj.getIAType();
            this.listType = new AOrderedListType(listType, "");
        }
        for (Object v : vals) {
            Class asxClass = PythonFunctionHelper.typeConv.get(v.getClass());
            try {
                Constructor cs = asxClass.getConstructor();
                IJObject obj = (IJObject) cs.newInstance();
                obj.setValue(v);
                add(obj);
            } catch (NoSuchMethodException ex) {
                ex.printStackTrace();
            } catch (IllegalAccessException ex) {
                ex.printStackTrace();
            } catch (InstantiationException ex) {
                ex.printStackTrace();
            } catch (InvocationTargetException ex) {
                ex.printStackTrace();
            }
        }

    }

    @Override
    public void serialize(DataOutput dataOutput, boolean writeTypeTag) throws HyracksDataException {
        IAsterixListBuilder listBuilder = new OrderedListBuilder();
        listBuilder.reset(listType);
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        for (IJObject jObject : jObjects) {
            fieldValue.reset();
            jObject.serialize(fieldValue.getDataOutput(), false);
            listBuilder.addItem(fieldValue);
        }
        listBuilder.write(dataOutput, writeTypeTag);

    }

    @Override
    public void reset() {
        jObjects.clear();
    }
}
