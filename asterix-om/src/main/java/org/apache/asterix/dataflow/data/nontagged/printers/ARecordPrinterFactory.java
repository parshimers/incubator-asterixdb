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

import java.io.PrintStream;

import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.printer.APrintVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;

public class ARecordPrinterFactory implements IPrinterFactory {

    private static final long serialVersionUID = 1L;
    private final ARecordType recType;

    public ARecordPrinterFactory(ARecordType recType) {
        this.recType = recType;
    }

    @Override
    public IPrinter createPrinter() {

        PointableAllocator allocator = new PointableAllocator();
        final IAType inputType = recType == null ? DefaultOpenFieldType.getDefaultOpenFieldType(ATypeTag.RECORD)
                : recType;
        final IVisitablePointable recAccessor = allocator.allocateRecordValue(inputType);
        final APrintVisitor printVisitor = new APrintVisitor();
        final Pair<PrintStream, ATypeTag> arg = new Pair<PrintStream, ATypeTag>(null, null);

        return new IPrinter() {

            @Override
            public void init() throws AlgebricksException {
                arg.second = inputType.getTypeTag();
            }

            @Override
            public void print(byte[] b, int start, int l, PrintStream ps) throws AlgebricksException {
                try {
                    recAccessor.set(b, start, l);
                    arg.first = ps;
                    recAccessor.accept(printVisitor, arg);
                } catch (Exception ioe) {
                    throw new AlgebricksException(ioe);
                }
            }
        };
    }
}
