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

import static org.apache.asterix.external.library.java.JTypeTag.OBJECT;
import static org.apache.asterix.om.types.ATypeTag.ARRAY;
import static org.apache.asterix.om.types.ATypeTag.MULTISET;
import static org.apache.asterix.om.types.BuiltinType.ANY;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import org.apache.asterix.external.api.IPyListAccessor;
import org.apache.asterix.external.api.IPyObjectAccessor;
import org.apache.asterix.external.api.IPyRecordAccessor;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PyObjectAccessors {

    private static final Logger LOGGER = LogManager.getLogger();

    private PyObjectAccessors() {
    }

    public static IPyObjectAccessor createFlatJObjectAccessor(ATypeTag aTypeTag) {
        IPyObjectAccessor accessor = null;
        switch (aTypeTag) {
            case BOOLEAN:
                accessor = new BooleanAccessor();
                break;
            case TINYINT:
                accessor = new ByteAccessor();
                break;
            case SMALLINT:
                accessor = new ShortAccessor();
                break;
            case INTEGER:
                accessor = new IntAccessor();
                break;
            case BIGINT:
                accessor = new LongAccessor();
                break;
            case FLOAT:
                accessor = new FloatAccessor();
                break;
            case DOUBLE:
                accessor = new DoubleAccessor();
                break;
            case STRING:
                accessor = new StringAccessor();
                break;
            case POINT:
            case POINT3D:
            case LINE:
                break;
            case DATE:
                accessor = new DateAccessor();
                break;
            case DATETIME:
                accessor = new DateTimeAccessor();
                break;
            case DURATION:
                //                accessor = new JDurationAccessor();
                break;
            case INTERVAL:
                //                accessor = new JIntervalAccessor();
                break;
            case CIRCLE:
            case POLYGON:
            case RECTANGLE:
                break;
            case TIME:
                accessor = new TimeAccessor();
                break;
            case NULL:
                accessor = new NullAccessor();
                break;
            case MISSING:
                accessor = new MissingAccessor();
                break;
            default:
                break;
        }
        return accessor;
    }

    public static class ByteAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return AInt8SerializerDeserializer.getByte(b, s + 1);
        }

    }

    public static class ShortAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return AInt16SerializerDeserializer.getShort(b, s + 1);
        }
    }

    public static class IntAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return AInt32SerializerDeserializer.getInt(b, s + 1);
        }
    }

    public static class NullAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            return BuiltinType.ANULL;
        }
    }

    public static class MissingAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            return BuiltinType.AMISSING;
        }
    }

    public static class LongAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return AInt64SerializerDeserializer.getLong(b, s + 1);
        }
    }

    public static class FloatAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return AFloatSerializerDeserializer.getFloat(b, s + 1);
        }
    }

    public static class DoubleAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return ADoubleSerializerDeserializer.getDouble(b, s + 1);
        }
    }

    public static class StringAccessor implements IPyObjectAccessor {
        private final UTF8StringReader reader = new UTF8StringReader();

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();

            String v;
            try {
                v = reader.readUTF(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            return v;
        }
    }

    public static class BooleanAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return ABooleanSerializerDeserializer.getBoolean(b, s + 1);
        }
    }

    public static class DateAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return ADateSerializerDeserializer.getChronon(b, s + 1);
        }
    }

    public static class DateTimeAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return ADateTimeSerializerDeserializer.getChronon(b, s + 1);
        }
    }

    public static class DurationAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            int l = pointable.getLength();
            return ADurationSerializerDeserializer.INSTANCE
                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
        }
    }

    public static class TimeAccessor implements IPyObjectAccessor {

        @Override
        public Object access(IPointable pointable) throws HyracksDataException {
            byte[] b = pointable.getByteArray();
            int s = pointable.getStartOffset();
            return ATimeSerializerDeserializer.getChronon(b, s + 1);
        }
    }

    //    public static class IntervalAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            long intervalStart = AIntervalSerializerDeserializer.getIntervalStart(b, s + 1);
    //            long intervalEnd = AIntervalSerializerDeserializer.getIntervalEnd(b, s + 1);
    //            byte intervalType = AIntervalSerializerDeserializer.getIntervalTimeType(b, s + 1);
    //            return null;
    //        }
    //    }
    //
    //    // Spatial Types
    //
    //    public static class CircleAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            ACircle v = ACircleSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            JPoint jpoint = (JPoint) objectPool.allocate(BuiltinType.APOINT);
    //            jpoint.setValue(v.getP().getX(), v.getP().getY());
    //            return jpoint;
    //        }
    //    }
    //
    //    public static class PointAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            APoint v = APointSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            return v;
    //        }
    //    }
    //
    //    public static class Point3DAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            APoint3D v = APoint3DSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            JPoint3D jObject = (JPoint3D) objectPool.allocate(BuiltinType.APOINT3D);
    //            jObject.setValue(v.getX(), v.getY(), v.getZ());
    //            return jObject;
    //        }
    //    }
    //
    //    public static class LineAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            ALine v = ALineSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            JLine jObject = (JLine) objectPool.allocate(BuiltinType.ALINE);
    //            jObject.setValue(v.getP1(), v.getP2());
    //            return jObject;
    //        }
    //    }
    //
    //    public static class PolygonAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            APolygon v = APolygonSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            JPolygon jObject = (JPolygon) objectPool.allocate(BuiltinType.APOLYGON);
    //            jObject.setValue(v.getPoints());
    //            return jObject;
    //        }
    //    }
    //
    //    public static class RectangleAccessor implements IPyObjectAccessor {
    //
    //        @Override
    //        public Object access(IPointable pointable)
    //                throws HyracksDataException {
    //            byte[] b = pointable.getByteArray();
    //            int s = pointable.getStartOffset();
    //            int l = pointable.getLength();
    //            ARectangle v = ARectangleSerializerDeserializer.INSTANCE
    //                    .deserialize(new DataInputStream(new ByteArrayInputStream(b, s + 1, l - 1)));
    //            JRectangle jObject = (JRectangle) objectPool.allocate(BuiltinType.ARECTANGLE);
    //            jObject.setValue(v.getP1(), v.getP2());
    //            return jObject;
    //        }
    //    }

    public static class PyRecordAccessor implements IPyRecordAccessor {

        private final UTF8StringReader reader = new UTF8StringReader();

        public PyRecordAccessor() {
        }

        @Override
        public Map<String, Object> access(ARecordVisitablePointable pointable, ARecordType recordType,
                PyObjectPointableVisitor pointableVisitor) throws HyracksDataException {
            ARecordVisitablePointable recordPointable = pointable;
            LinkedHashMap<String, Object> fields;
            List<IVisitablePointable> fieldPointables = recordPointable.getFieldValues();
            List<IVisitablePointable> fieldTypeTags = recordPointable.getFieldTypeTags();
            List<IVisitablePointable> fieldNames = recordPointable.getFieldNames();
            StringAccessor sa = new StringAccessor();
            int index = 0;
            fields = null;
            boolean closedPart;
            try {
                Object fieldObject = null;
                for (IPointable fieldPointable : fieldPointables) {
                    closedPart = index < recordType.getFieldTypes().length;
                    IPointable tt = fieldTypeTags.get(index);
                    ATypeTag typeTag =
                            EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(tt.getByteArray()[tt.getStartOffset()]);
                    IAType fieldType;
                    fieldType =
                            closedPart ? recordType.getFieldTypes()[index] : TypeTagUtil.getBuiltinTypeByTag(typeTag);
                    switch (typeTag) {
                        case OBJECT:
                            fieldObject = pointableVisitor.visit((ARecordVisitablePointable) fieldPointable, fieldType);
                            break;
                        case ARRAY:
                        case MULTISET:
                            if (fieldPointable instanceof AFlatValuePointable) {
                                // value is null
                                fieldObject = null;
                            } else {
                                fieldObject =
                                        pointableVisitor.visit((AListVisitablePointable) fieldPointable, fieldType);
                            }
                            break;
                        case ANY:
                            break;
                        default:
                            fieldObject = pointableVisitor.visit((AFlatValuePointable) fieldPointable, fieldType);
                    }
                    if (closedPart) {
                        fields.put(recordType.getFieldNames()[index], fieldObject);
                    } else {
                        fields.put(((String) sa.access(fieldNames.get(index))), fieldObject);
                    }
                    index++;
                    fieldObject = null;
                }

            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Failure while accessing a java record", e);
                throw HyracksDataException.create(e);
            }
            return fields;
        }

        public void reset() throws HyracksDataException {
        }

    }

    public static class PyListAccessor implements IPyListAccessor {

        public PyListAccessor() {
        }

        @Override
        public Object access(AListVisitablePointable pointable, IAType listType,
                PyObjectPointableVisitor pointableVisitor) throws HyracksDataException {
            List<IVisitablePointable> items = pointable.getItems();
            List<IVisitablePointable> itemTags = pointable.getItemTags();
            Collection<Object> list = pointable.ordered() ? new ArrayList<>() : new HashSet<>();
            Object listItem;
            for (int iter1 = 0; iter1 < items.size(); iter1++) {
                IVisitablePointable itemPointable = items.get(iter1);
                // First, try to get defined type.
                IAType fieldType = ((AbstractCollectionType) listType).getItemType();
                if (fieldType.getTypeTag() == ATypeTag.ANY) {
                    // Second, if defined type is not available, try to infer it from data
                    IVisitablePointable itemTagPointable = itemTags.get(iter1);
                    ATypeTag itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                            .deserialize(itemTagPointable.getByteArray()[itemTagPointable.getStartOffset()]);
                    fieldType = TypeTagUtil.getBuiltinTypeByTag(itemTypeTag);
                }
                switch (listType.getTypeTag()) {
                    case OBJECT:
                        listItem = pointableVisitor.visit((ARecordVisitablePointable) itemPointable, listType);
                        break;
                    case MULTISET:
                    case ARRAY:
                        listItem = pointableVisitor.visit((AListVisitablePointable) itemPointable, listType);
                        break;
                    case ANY:
                        throw new RuntimeDataException(ErrorCode.LIBRARY_JOBJECT_ACCESSOR_CANNOT_PARSE_TYPE,
                                listType.getTypeTag());
                    default:
                        listItem = pointableVisitor.visit((AFlatValuePointable) itemPointable, listType);
                }
                list.add(listItem);
            }
            return list;
        }
    }
}
