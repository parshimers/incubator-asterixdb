package edu.uci.ics.asterix.metadata.feeds;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class TimestampedDelimitedDataParser extends DelimitedDataParser {

    private final FeedFrameTupleDecorator tupleDecorator;

    public TimestampedDelimitedDataParser(ARecordType recordType, IValueParserFactory[] valueParserFactories,
            char fieldDelimter, int partition) {
        super(recordType, valueParserFactories, fieldDelimter);
        this.tupleDecorator = new FeedFrameTupleDecorator(partition);
    }

    protected void writeRecord(IARecordBuilder recordBuilder, DataOutput out, boolean writeTypeTag) throws IOException,
            AsterixException {

        if (recordBuilder.getFieldId(FeedConstants.StatisticsConstants.INTAKE_TIMESTAMP) > 0) {
            super.writeDataRecord(recordBuilder, out, writeTypeTag);
        } else {
            tupleDecorator.addTupleId(recordBuilder);
            tupleDecorator.addIntakePartition(recordBuilder);
            tupleDecorator.addIntakeTimestamp(recordBuilder);
            tupleDecorator.addStoreTimestamp(recordBuilder);
            super.writeDataRecord(recordBuilder, out, writeTypeTag);
        }
    }

}