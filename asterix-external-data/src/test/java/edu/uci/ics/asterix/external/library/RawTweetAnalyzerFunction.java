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

import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JPoint;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;

public class RawTweetAnalyzerFunction implements IExternalScalarFunction {

    private JUnorderedList list = null;
    private JPoint location = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
        location = new JPoint(0, 0);
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        list.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString text = (JString) inputRecord.getValueByName(InputTypeConstants.MESSAGE);
        JDouble latitude = (JDouble) inputRecord.getValueByName(InputTypeConstants.LATITUDE);
        JDouble longitude = (JDouble) inputRecord.getValueByName(InputTypeConstants.LONGITUDE);

        if (latitude != null && longitude != null) {
            location.setValue(latitude.getValue(), longitude.getValue());
        }
        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }

        JRecord outputRecord = (JRecord) functionHelper.getResultObject();
        outputRecord.setField(InputTypeConstants.ID, inputRecord.getValueByName(InputTypeConstants.ID));

        JRecord userRecord = (JRecord) inputRecord.getValueByName(InputTypeConstants.USER);
        outputRecord.setField(OutputTypeConstants.USER_NAME, userRecord.getValueByName(InputTypeConstants.SCREEN_NAME));

        outputRecord.setField(OutputTypeConstants.LOCATION, location);

        outputRecord
                .setField(OutputTypeConstants.CREATED_AT, inputRecord.getValueByName(InputTypeConstants.CREATED_AT));

        outputRecord.setField(InputTypeConstants.MESSAGE, text);
        outputRecord.setField(OutputTypeConstants.TOPICS, list);

        inputRecord.addField("topics", list);
        functionHelper.setResult(outputRecord);
    }

    private static class InputTypeConstants {
        public static final String MESSAGE = "message-text";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";

        public static final String ID = "id";
        public static final String USER = "user";
        public static final String CREATED_AT = "created_at";
        public static final String SCREEN_NAME = "screen_name";
    }

    private static class OutputTypeConstants {
        public static final String USER_NAME = "user_name";
        public static final String LOCATION = "location";
        public static final String CREATED_AT = "created_at";
        public static final String TOPICS = "topics";
    }

}
