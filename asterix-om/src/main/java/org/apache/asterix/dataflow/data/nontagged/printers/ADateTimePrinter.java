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
package org.apache.asterix.dataflow.data.nontagged.printers;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.base.temporal.GregorianCalendarSystem.Fields;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;

public class ADateTimePrinter implements IPrinter {

    public static final ADateTimePrinter INSTANCE = new ADateTimePrinter();
    private static final GregorianCalendarSystem gCalInstance = GregorianCalendarSystem.getInstance();

    @Override
    public void init() {

    }

    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        ps.print("datetime(\"");

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

        ps.print("\")");
    }

    public void printString(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        long chrononTime = AInt64SerializerDeserializer.getLong(b, s + 1);

        try {
            gCalInstance.getExtendStringRepUntilField(chrononTime, 0, ps, Fields.YEAR, Fields.MILLISECOND, true);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }

    }
}