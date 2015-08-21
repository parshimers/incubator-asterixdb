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
package org.apache.asterix.dataflow.data.nontagged.printers.csv;

import java.io.PrintStream;

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.data.IPrinter;

public class AYearMonthDurationPrinter implements IPrinter {

    public static final AYearMonthDurationPrinter INSTANCE = new AYearMonthDurationPrinter();

    /* (non-Javadoc)
     * @see org.apache.hyracks.algebricks.data.IPrinter#init()
     */
    @Override
    public void init() throws AlgebricksException {
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.algebricks.data.IPrinter#print(byte[], int, int, java.io.PrintStream)
     */
    @Override
    public void print(byte[] b, int s, int l, PrintStream ps) throws AlgebricksException {
        int months = AInt32SerializerDeserializer.getInt(b, s + 1);
        // QQQ reasonable CSV representation?
        ps.print(months);
    }

}
