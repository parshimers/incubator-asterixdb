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
package edu.uci.ics.asterix.external.library;

import java.util.Iterator;

import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JTypeTag;

public class ListConcatFunction implements IExternalScalarFunction {

    private JString result;

    @Override
    public void deinitialize() {
        // nothing to do here
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        result.setValue("");
        JOrderedList arg0 = ((JOrderedList) functionHelper.getArgument(0));
        Iterator<IJObject> iterator = arg0.iterator();
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        while (iterator.hasNext()) {
            if (!first) {
                builder.append(" ");
            } else {
                first = false;
            }
            builder.append(((JString) iterator.next()).getValue());
        }
        result.setValue(new String(builder));
        functionHelper.setResult(result);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        result = (JString) functionHelper.getObject(JTypeTag.STRING);
    }
}
