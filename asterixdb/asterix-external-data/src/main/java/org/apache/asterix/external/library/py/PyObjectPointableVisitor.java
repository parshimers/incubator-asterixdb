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
package org.apache.asterix.external.library.py;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.api.IPyListAccessor;
import org.apache.asterix.external.api.IPyObjectAccessor;
import org.apache.asterix.external.api.IPyRecordAccessor;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PyObjectPointableVisitor implements IVisitablePointableVisitor<Object, IAType> {

    private final Map<ATypeTag, IPyObjectAccessor> flatJObjectAccessors = new HashMap<>();
    private final Map<IVisitablePointable, IPyRecordAccessor> raccessorToJObject = new HashMap<>();
    private final Map<IVisitablePointable, IPyListAccessor> laccessorToPrinter = new HashMap<>();

    @Override
    public Object visit(AListVisitablePointable accessor, IAType type) throws HyracksDataException {
        IPyListAccessor listAccessor = laccessorToPrinter.get(accessor);
        if (listAccessor == null) {
            listAccessor = new PyObjectAccessors.PyListAccessor();
            laccessorToPrinter.put(accessor, listAccessor);
        }
        return listAccessor.access(accessor, type, this);
    }

    @Override
    public Object visit(ARecordVisitablePointable accessor, IAType type) throws HyracksDataException {
        IPyRecordAccessor jRecordAccessor = raccessorToJObject.get(accessor);
        if (jRecordAccessor == null) {
            jRecordAccessor = new PyObjectAccessors.PyRecordAccessor();
            raccessorToJObject.put(accessor, jRecordAccessor);
        }
        return jRecordAccessor.access(accessor, (ARecordType) type, this);
    }

    @Override
    public Object visit(AFlatValuePointable accessor, IAType type) throws HyracksDataException {
        IPyObjectAccessor jObjectAccessor = flatJObjectAccessors.get(type.getTypeTag());
        if (jObjectAccessor == null) {
            jObjectAccessor = PyObjectAccessors.createFlatJObjectAccessor(type.getTypeTag());
            flatJObjectAccessors.put(type.getTypeTag(), jObjectAccessor);
        }

        return jObjectAccessor.access(accessor);
    }
}
