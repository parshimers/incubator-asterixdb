package edu.uci.ics.asterix.metadata.feeds;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.FeedConstants.StatisticsConstants;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class TimestampedADMDataParser extends ADMDataParser {

    public TimestampedADMDataParser() {
        super();
    }

    protected void writeRecord(IARecordBuilder recordBuilder, DataOutput out, boolean writeTypeTag) throws IOException,
            AsterixException {

        if (recordBuilder.getFieldId(FeedConstants.StatisticsConstants.INTAKE_TIMESTAMP) > 0) {
            super.writeRecord(recordBuilder, out, writeTypeTag);
        } else {
            long currentTime = System.currentTimeMillis();
            addAttribute(StatisticsConstants.INTAKE_TIMESTAMP, currentTime, recordBuilder);
            addAttribute(StatisticsConstants.STORE_TIMESTAMP, currentTime, recordBuilder);
            super.writeRecord(recordBuilder, out, writeTypeTag);
        }
    }

    private void addAttribute(String attrName, long attrValue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {
        ArrayBackedValueStorage attrNameStorage = new ArrayBackedValueStorage();
        attrNameStorage.reset();
        aString.setValue(attrName);
        stringSerde.serialize(aString, attrNameStorage.getDataOutput());

        ArrayBackedValueStorage attrValueStorage = new ArrayBackedValueStorage();
        attrValueStorage.reset();
        aInt64.setValue(attrValue);
        int64Serde.serialize(aInt64, attrValueStorage.getDataOutput());

        recordBuilder.addField(attrNameStorage, attrValueStorage);
    }
}