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

package org.apache.asterix.om.pointables.printer.csv;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * This class is to print the content of a record. It is ONLY visible to
 * APrintVisitor.
 */
class ARecordPrinter {
    // QQQ Might we want to make this a configurable delimiter?
    private static String COMMA = ",";

    private final Pair<PrintStream, ATypeTag> nameVisitorArg = new Pair<PrintStream, ATypeTag>(null, ATypeTag.STRING);
    private final Pair<PrintStream, ATypeTag> itemVisitorArg = new Pair<PrintStream, ATypeTag>(null, null);

    public ARecordPrinter() {

    }

    public void printRecord(ARecordVisitablePointable recordAccessor, PrintStream ps, APrintVisitor visitor)
            throws IOException, AsterixException {
        List<IVisitablePointable> fieldNames = recordAccessor.getFieldNames();
        List<IVisitablePointable> fieldTags = recordAccessor.getFieldTypeTags();
        List<IVisitablePointable> fieldValues = recordAccessor.getFieldValues();

        nameVisitorArg.first = ps;
        itemVisitorArg.first = ps;

        // print field 0 to n-2
        for (int i = 0; i < fieldNames.size() - 1; i++) {
            printField(ps, visitor, fieldNames, fieldTags, fieldValues, i);
            // print the comma
            ps.print(COMMA);
        }

        // print field n-1
        if (fieldValues.size() > 0) {
            printField(ps, visitor, fieldNames, fieldTags, fieldValues, fieldValues.size() - 1);
        }
    }

    private void printField(PrintStream ps, APrintVisitor visitor, List<IVisitablePointable> fieldNames,
            List<IVisitablePointable> fieldTags, List<IVisitablePointable> fieldValues, int i) throws AsterixException {
        IVisitablePointable itemTypeTag = fieldTags.get(i);
        IVisitablePointable item = fieldValues.get(i);
        ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(itemTypeTag.getByteArray()[itemTypeTag.getStartOffset()]);
        itemVisitorArg.second = item.getLength() <= 1 ? ATypeTag.NULL : typeTag;

        // print field value
        item.accept(visitor, itemVisitorArg);
    }
}
