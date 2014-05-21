package edu.uci.ics.asterix.metadata.feeds;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.common.feeds.FeedConstants.StatisticsConstants;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class TimestampedDelimitedDataParser extends DelimitedDataParser {

    private final String nodeId;
    private final ArrayBackedValueStorage attrNameStorage;
    private final ArrayBackedValueStorage attrValueStorage;

    public TimestampedDelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories,
            char fieldDelimter) {
        super(recordType, valueParserFactories, fieldDelimter);
        this.nodeId = "";
        this.attrNameStorage = new ArrayBackedValueStorage();
        this.attrValueStorage = new ArrayBackedValueStorage();
    }

    protected void writeRecord(IARecordBuilder recordBuilder, DataOutput out, boolean writeTypeTag) throws IOException,
            AsterixException {

        if (recordBuilder.getFieldId(FeedConstants.StatisticsConstants.INTAKE_TIMESTAMP) > 0) {
            super.writeDataRecord(recordBuilder, out, writeTypeTag);
        } else {
            long currentTime = System.currentTimeMillis();
            addStringAttribute(StatisticsConstants.INTAKE_NC_ID, nodeId, recordBuilder);
            addLongAttribute(StatisticsConstants.INTAKE_TIMESTAMP, currentTime, recordBuilder);
            addLongAttribute(StatisticsConstants.STORE_TIMESTAMP, currentTime, recordBuilder);
            super.writeDataRecord(recordBuilder, out, writeTypeTag);
        }
    }

    private void addLongAttribute(String attrName, long attrValue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {
        attrNameStorage.reset();
        aString.setValue(attrName);
        stringSerde.serialize(aString, attrNameStorage.getDataOutput());

        attrValueStorage.reset();
        aInt64.setValue(attrValue);
        int64Serde.serialize(aInt64, attrValueStorage.getDataOutput());

        recordBuilder.addField(attrNameStorage, attrValueStorage);
    }

    private void addStringAttribute(String attrName, String attrValue, IARecordBuilder recordBuilder)
            throws HyracksDataException, AsterixException {
        attrNameStorage.reset();
        aString.setValue(attrName);
        stringSerde.serialize(aString, attrNameStorage.getDataOutput());

        attrValueStorage.reset();
        aString.setValue(attrValue);
        stringSerde.serialize(aString, attrValueStorage.getDataOutput());

        recordBuilder.addField(attrNameStorage, attrValueStorage);
    }
}