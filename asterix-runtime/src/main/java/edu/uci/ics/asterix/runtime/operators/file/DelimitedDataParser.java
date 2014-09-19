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
package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataParser extends AbstractDataParser implements IDataParser {

    protected final IValueParserFactory[] valueParserFactories;
    protected final char fieldDelimiter;
    protected final char quote;
    protected final ARecordType recordType;

    private IARecordBuilder recBuilder;
    private ArrayBackedValueStorage fieldValueBuffer;
    private DataOutput fieldValueBufferOutput;
    private IValueParser[] valueParsers;
    private FieldCursor cursor;
    private byte[] fieldTypeTags;
    private int[] fldIds;
    private ArrayBackedValueStorage[] nameBuffers;
    private byte AUUIDTag = ATypeTag.UUID.serialize();

    // Variables used to set a UUID for the auto-generated PK field
    private boolean isPKAutoGenerated;
    private int primaryKeyPosition;
    private final ARecordType origRecordTypeForAutoGeneratedPK;

    private boolean areAllNullFields;
    private boolean isDoubleQuoteIncludedInThisField;
    private int doubleQuoteCount;

    private int lineCount;
    private int fieldCount;

    public DelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories, char fieldDelimter,
            char quote) {
        this(recordType, valueParserFactories, fieldDelimter, quote, false, -1, null);
    }

    public DelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories, char fieldDelimter,
            char quote, boolean isPKAutoGenerated, int primaryKeyPosition, ARecordType origRecordTypeForAutoGeneratedPK) {
        this.recordType = recordType;
        this.valueParserFactories = valueParserFactories;
        this.fieldDelimiter = fieldDelimter;
        this.quote = quote;
        this.isPKAutoGenerated = isPKAutoGenerated;
        this.primaryKeyPosition = primaryKeyPosition;
        this.origRecordTypeForAutoGeneratedPK = origRecordTypeForAutoGeneratedPK;
    }

    @Override
    public void initialize(InputStream in, ARecordType recordType, boolean datasetRec) throws AsterixException,
            IOException {

        ARecordType recordTypeToApply = null;
        if (isPKAutoGenerated)
            recordTypeToApply = origRecordTypeForAutoGeneratedPK;
        else
            recordTypeToApply = recordType;

        lineCount = 1;

        valueParsers = new IValueParser[valueParserFactories.length];
        for (int i = 0; i < valueParserFactories.length; ++i) {
            valueParsers[i] = valueParserFactories[i].createValueParser();
        }

        isDoubleQuoteIncludedInThisField = false;

        fieldValueBuffer = new ArrayBackedValueStorage();
        fieldValueBufferOutput = fieldValueBuffer.getDataOutput();

        // If PK is auto-generated, then we need to use the recordType that
        // includes PK, since recordType variable does not include PK field.
        recBuilder = new RecordBuilder();
        recBuilder.reset(recordTypeToApply);
        recBuilder.init();

        int n = recordType.getFieldNames().length;
        fieldTypeTags = new byte[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
            fieldTypeTags[i] = tag.serialize();
        }

        fldIds = new int[n];
        nameBuffers = new ArrayBackedValueStorage[n];
        AMutableString str = new AMutableString(null);
        for (int i = 0; i < n; i++) {
            String name = recordType.getFieldNames()[i];
            fldIds[i] = recBuilder.getFieldId(name);
            if (fldIds[i] < 0) {
                if (!recordType.isOpen()) {
                    throw new HyracksDataException("Illegal field " + name + " in closed type " + recordType);
                } else {
                    nameBuffers[i] = new ArrayBackedValueStorage();
                    fieldNameToBytes(name, str, nameBuffers[i]);
                }
            }
        }

        cursor = new FieldCursor(new InputStreamReader(in));

    }

    @Override
    public boolean parse(DataOutput out) throws AsterixException, IOException {
        while (cursor.nextRecord()) {
            // If PK is auto-generated, then we need to use the recordType that
            // includes PK, since recordType variable does not include PK field.
            if (isPKAutoGenerated)
                recBuilder.reset(origRecordTypeForAutoGeneratedPK);
            else
                recBuilder.reset(recordType);

            recBuilder.init();
            areAllNullFields = true;

            fieldCount = 0;

            for (int i = 0; i < valueParsers.length; ++i) {
                if (!cursor.nextField()) {
                    break;
                }
                fieldValueBuffer.reset();

                if (cursor.fStart == cursor.fEnd && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.STRING
                        && recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.NULL) {
                    // if the field is empty and the type is optional, insert
                    // NULL. Note that string type can also process empty field as an
                    // empty string
                    if (recordType.getFieldTypes()[i].getTypeTag() != ATypeTag.UNION
                            || !NonTaggedFormatUtil.isOptionalField((AUnionType) recordType.getFieldTypes()[i])) {
                        throw new AsterixException("At line: " + lineCount + " - Field " + i
                                + " is not an optional type so it cannot accept null value. ");
                    }
                    fieldValueBufferOutput.writeByte(ATypeTag.NULL.serialize());
                    ANullSerializerDeserializer.INSTANCE.serialize(ANull.NULL, out);
                } else {
                    fieldValueBufferOutput.writeByte(fieldTypeTags[i]);
                    // Eliminate doule quotes in the field that we are going to parse
                    if (isDoubleQuoteIncludedInThisField) {
                        eliminateDoubleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                        cursor.fEnd -= doubleQuoteCount;
                        isDoubleQuoteIncludedInThisField = false;
                    }
                    valueParsers[i].parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart,
                            fieldValueBufferOutput);
                    areAllNullFields = false;
                }
                if (fldIds[i] < 0) {
                    recBuilder.addField(nameBuffers[i], fieldValueBuffer);
                } else {
                    recBuilder.addField(fldIds[i], fieldValueBuffer);
                }

                fieldCount++;

            }

            // Should not have any more fields now.
            // We parsed all fields except an auto-generated PK at this point.
            // We now create a new UUID and assign it as a PK.
            if (isPKAutoGenerated && fieldCount == origRecordTypeForAutoGeneratedPK.getFieldTypes().length - 1) {
                fieldValueBuffer.reset();
                aUUID.nextUUID();
                fieldValueBufferOutput.writeByte(AUUIDTag);
                Integer64SerializerDeserializer.INSTANCE.serialize(aUUID.getMostSignificantBits(),
                        fieldValueBufferOutput);
                Integer64SerializerDeserializer.INSTANCE.serialize(aUUID.getLeastSignificantBits(),
                        fieldValueBufferOutput);
                recBuilder.addField(primaryKeyPosition, fieldValueBuffer);
                areAllNullFields = false;
            } else if (isPKAutoGenerated && fieldCount >= origRecordTypeForAutoGeneratedPK.getFieldTypes().length) {
                // If we have all fields in the file including auto-generated PK,
                // throw an exception
                throw new AsterixException("At line: " + lineCount
                        + " - check number of fields. Auto-generated PK field should not exist in the input data.");
            }

            if (!areAllNullFields) {
                recBuilder.write(out, true);
                return true;
            }
        }
        return false;
    }

    protected void fieldNameToBytes(String fieldName, AMutableString str, ArrayBackedValueStorage buffer)
            throws HyracksDataException {
        buffer.reset();
        DataOutput out = buffer.getDataOutput();
        str.setValue(fieldName);
        try {
            stringSerde.serialize(str, out);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    protected enum State {
        INIT,
        IN_RECORD,
        EOR,
        CR,
        EOF
    }

    protected class FieldCursor {
        private static final int INITIAL_BUFFER_SIZE = 4096;
        private static final int INCREMENT = 4096;

        private final Reader in;

        private char[] buffer;
        private int start;
        private int end;
        private State state;

        private int fStart;
        private int fEnd;

        private int lastQuotePosition;
        private int lastDoubleQuotePosition;
        private int lastDelimiterPosition;
        private int quoteCount;
        private boolean startedQuote;

        public FieldCursor(Reader in) {
            this.in = in;
            buffer = new char[INITIAL_BUFFER_SIZE];
            start = 0;
            end = 0;
            state = State.INIT;
            lastDelimiterPosition = -99;
            lastQuotePosition = -99;
            lastDoubleQuotePosition = -99;
            quoteCount = 0;
            startedQuote = false;
        }

        public boolean nextRecord() throws IOException {
            while (true) {
                switch (state) {
                    case INIT:
                        boolean eof = !readMore();
                        if (eof) {
                            state = State.EOF;
                            return false;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case IN_RECORD:
                        int p = start;
                        while (true) {
                            if (p >= end) {
                                int s = start;
                                eof = !readMore();
                                if (eof) {
                                    state = State.EOF;
                                    return start < end;
                                }
                                p -= (s - start);
                                lastQuotePosition -= (s - start);
                                lastDoubleQuotePosition -= (s - start);
                                lastDelimiterPosition -= (s - start);
                            }
                            char ch = buffer[p];
                            // We perform rough format correctness (delimiter, quote) check here
                            // to set the starting position of a record.
                            // In the field level, more checking will be conducted.
                            if (ch == quote) {
                                startedQuote = true;
                                // check two quotes in a row - "". This is an escaped quote
                                if (lastQuotePosition == p - 1 && start != p - 1 && lastDoubleQuotePosition != p - 1) {
                                    lastDoubleQuotePosition = p;
                                }
                                lastQuotePosition = p;
                            } else if (ch == fieldDelimiter) {
                                if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1) {
                                    startedQuote = false;
                                    lastDelimiterPosition = p;
                                }
                            } else if (ch == '\n' && !startedQuote) {
                                start = p + 1;
                                state = State.EOR;
                                lastDelimiterPosition = p;
                                break;
                            } else if (ch == '\r' && !startedQuote) {
                                start = p + 1;
                                state = State.CR;
                                break;
                            }
                            ++p;
                        }
                        break;

                    case CR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        char ch = buffer[start];
                        if (ch == '\n' && !startedQuote) {
                            ++start;
                            state = State.EOR;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case EOR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        state = State.IN_RECORD;
                        lastDelimiterPosition = start;
                        return start < end;

                    case EOF:
                        return false;
                }
            }
        }

        public boolean nextField() throws IOException {
            switch (state) {
                case INIT:
                case EOR:
                case EOF:
                case CR:
                    return false;

                case IN_RECORD:
                    boolean eof;
                    // reset quote related values
                    startedQuote = false;
                    isDoubleQuoteIncludedInThisField = false;
                    lastQuotePosition = -99;
                    lastDoubleQuotePosition = -99;
                    quoteCount = 0;
                    doubleQuoteCount = 0;

                    int p = start;
                    while (true) {
                        if (p >= end) {
                            int s = start;
                            eof = !readMore();
                            p -= (s - start);
                            lastQuotePosition -= (s - start);
                            lastDoubleQuotePosition -= (s - start);
                            lastDelimiterPosition -= (s - start);
                            if (eof) {
                                state = State.EOF;
                                if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                        && quoteCount == doubleQuoteCount * 2 + 2) {
                                    // set the position of fStart to +1, fEnd to -1 to remove quote character
                                    fStart = start + 1;
                                    fEnd = p - 1;
                                } else {
                                    fStart = start;
                                    fEnd = p;
                                }
                                return true;
                            }
                        }
                        char ch = buffer[p];
                        if (ch == quote) {
                            // If this is first quote in the field, then it needs to be placed in the beginning.
                            if (!startedQuote) {
                                if (lastDelimiterPosition == p - 1 || lastDelimiterPosition == -99) {
                                    startedQuote = true;
                                } else {
                                    // In this case, we don't have a quote in the beginning of a field.
                                    throw new IOException(
                                            "At line: "
                                                    + lineCount
                                                    + ", field#: "
                                                    + (fieldCount+1)
                                                    + " - a quote enclosing a field needs to be placed in the beginning of that field.");
                                }
                            }
                            // Check double quotes - "". We check [start != p-2]
                            // to avoid false positive where there is no value in a field,
                            // since it looks like a double quote. However, it's not a double quote.
                            // (e.g. if field2 has no value:
                            //       field1,"",field3 ... )
                            if (lastQuotePosition == p - 1 && lastDelimiterPosition != p - 2
                                    && lastDoubleQuotePosition != p - 1) {
                                isDoubleQuoteIncludedInThisField = true;
                                doubleQuoteCount++;
                                lastDoubleQuotePosition = p;
                            }
                            lastQuotePosition = p;
                            quoteCount++;
                        } else if (ch == fieldDelimiter) {
                            // If there was no quote in the field,
                            // then we assume that the field contains a valid string.
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote) {
                                if (lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1) {
                                    // There is a quote right before the delimiter (e.g. ",)  and it is not two quote,
                                    // then the field contains a valid string.
                                    // We set the position of fStart to +1, fEnd to -1 to remove quote character
                                    fStart = start + 1;
                                    fEnd = p - 1;
                                    start = p + 1;
                                    lastDelimiterPosition = p;
                                    return true;
                                } else if (lastQuotePosition < p - 1 && lastQuotePosition != lastDoubleQuotePosition
                                        && quoteCount == doubleQuoteCount * 2 + 2) {
                                    // There is a quote before the delimiter, however it is not directly placed before the delimiter.
                                    // In this case, we throw an exception.
                                    // quoteCount == doubleQuoteCount * 2 + 2 : only true when we have two quotes except double-quotes.
                                    throw new IOException("At line: " + lineCount + ", field#: " + (fieldCount+1)
                                            + " -  A quote enclosing a field needs to be followed by the delimiter.");
                                }
                            }
                            // If the control flow reaches here: we have a delimiter in this field and
                            // there should be a quote in the beginning and the end of
                            // this field. So, just continue reading next character
                        } else if (ch == '\n') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                state = State.EOR;
                                lineCount++;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                    && quoteCount == doubleQuoteCount * 2 + 2) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                lastDelimiterPosition = p;
                                start = p + 1;
                                state = State.EOR;
                                lineCount++;
                                return true;
                            }
                        } else if (ch == '\r') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                state = State.CR;
                                lastDelimiterPosition = p;
                                return true;
                            } else if (startedQuote && lastQuotePosition == p - 1 && lastDoubleQuotePosition != p - 1
                                    && quoteCount == doubleQuoteCount * 2 + 2) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                lastDelimiterPosition = p;
                                start = p + 1;
                                state = State.CR;
                                return true;
                            }
                        }
                        ++p;
                    }
            }
            throw new IllegalStateException();
        }

        protected boolean readMore() throws IOException {
            if (start > 0) {
                System.arraycopy(buffer, start, buffer, 0, end - start);
            }
            end -= start;
            start = 0;

            if (end == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length + INCREMENT);
            }

            int n = in.read(buffer, end, buffer.length - end);
            if (n < 0) {
                return false;
            }
            end += n;
            return true;
        }

    }

    // Eliminate escaped double quotes("") in a field
    protected void eliminateDoubleQuote(char[] buffer, int start, int length) {
        int lastDoubleQuotePosition = -99;
        int writepos = start;
        int readpos = start;
        // Find positions where double quotes appear
        for (int i = 0; i < length; i++) {
            // Skip double quotes
            if (buffer[readpos] == quote && lastDoubleQuotePosition != readpos - 1) {
                lastDoubleQuotePosition = readpos;
                readpos++;
            } else {
                // Moving characters except double quote to the front
                if (writepos != readpos) {
                    buffer[writepos] = buffer[readpos];
                }
                writepos++;
                readpos++;
            }
        }
    }
}
