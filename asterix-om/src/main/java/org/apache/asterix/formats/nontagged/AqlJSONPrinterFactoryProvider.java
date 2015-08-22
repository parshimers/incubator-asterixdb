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
package org.apache.asterix.formats.nontagged;

import org.apache.asterix.dataflow.data.nontagged.printers.ABinaryPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.AUUIDPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.ShortWithoutTypeInfoPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ABooleanPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ACirclePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ADatePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ADateTimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ADayTimeDurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ADoublePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ADurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AFloatPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AInt16PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AInt32PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AInt64PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AInt8PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AIntervalPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ALinePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ANullPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ANullableFieldPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AOrderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.APoint3DPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.APointPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.APolygonPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ARectanglePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AStringPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.ATimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AUnionPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AUnorderedlistPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.json.AYearMonthDurationPrinterFactory;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;

public class AqlJSONPrinterFactoryProvider implements IPrinterFactoryProvider {

    public static final AqlJSONPrinterFactoryProvider INSTANCE = new AqlJSONPrinterFactoryProvider();

    private AqlJSONPrinterFactoryProvider() {
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object type) throws AlgebricksException {
        IAType aqlType = (IAType) type;

        if (aqlType != null) {
            switch (aqlType.getTypeTag()) {
                case INT8:
                    return AInt8PrinterFactory.INSTANCE;
                case INT16:
                    return AInt16PrinterFactory.INSTANCE;
                case INT32:
                    return AInt32PrinterFactory.INSTANCE;
                case INT64:
                    return AInt64PrinterFactory.INSTANCE;
                case NULL:
                    return ANullPrinterFactory.INSTANCE;
                case BOOLEAN:
                    return ABooleanPrinterFactory.INSTANCE;
                case FLOAT:
                    return AFloatPrinterFactory.INSTANCE;
                case DOUBLE:
                    return ADoublePrinterFactory.INSTANCE;
                case TIME:
                    return ATimePrinterFactory.INSTANCE;
                case DATE:
                    return ADatePrinterFactory.INSTANCE;
                case DATETIME:
                    return ADateTimePrinterFactory.INSTANCE;
                case DURATION:
                    return ADurationPrinterFactory.INSTANCE;
                case YEARMONTHDURATION:
                    return AYearMonthDurationPrinterFactory.INSTANCE;
                case DAYTIMEDURATION:
                    return ADayTimeDurationPrinterFactory.INSTANCE;
                case INTERVAL:
                    return AIntervalPrinterFactory.INSTANCE;
                case POINT:
                    return APointPrinterFactory.INSTANCE;
                case POINT3D:
                    return APoint3DPrinterFactory.INSTANCE;
                case LINE:
                    return ALinePrinterFactory.INSTANCE;
                case POLYGON:
                    return APolygonPrinterFactory.INSTANCE;
                case CIRCLE:
                    return ACirclePrinterFactory.INSTANCE;
                case RECTANGLE:
                    return ARectanglePrinterFactory.INSTANCE;
                case STRING:
                    return AStringPrinterFactory.INSTANCE;
                case BINARY:
                    return ABinaryPrinterFactory.INSTANCE;
                case RECORD:
                    return new ARecordPrinterFactory((ARecordType) aqlType);
                case ORDEREDLIST:
                    return new AOrderedlistPrinterFactory((AOrderedListType) aqlType);
                case UNORDEREDLIST:
                    return new AUnorderedlistPrinterFactory((AUnorderedListType) aqlType);
                case UNION: {
                    if (((AUnionType) aqlType).isNullableType())
                        return new ANullableFieldPrinterFactory((AUnionType) aqlType);
                    else
                        return new AUnionPrinterFactory((AUnionType) aqlType);
                }
                case UUID: {
                    return AUUIDPrinterFactory.INSTANCE;
                }
                case SHORTWITHOUTTYPEINFO:
                    return ShortWithoutTypeInfoPrinterFactory.INSTANCE;
                case ANY:
                case BITARRAY:
                case ENUM:
                case SPARSERECORD:
                case SYSTEM_NULL:
                case TYPE:
                case UINT16:
                case UINT32:
                case UINT64:
                case UINT8:
                case UUID_STRING:
                    // These types are not intended to be printed to the user.
                    break;
            }
        }
        return AObjectPrinterFactory.INSTANCE;
    }
}
