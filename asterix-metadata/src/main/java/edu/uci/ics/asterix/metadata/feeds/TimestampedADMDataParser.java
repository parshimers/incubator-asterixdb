package edu.uci.ics.asterix.metadata.feeds;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class TimestampedADMDataParser extends ADMDataParser {

    private final FeedFrameTupleDecorator tupleDecorator;

    public TimestampedADMDataParser(IHyracksTaskContext ctx, int partition) {
        super();
        this.tupleDecorator = new FeedFrameTupleDecorator(partition);
    }

    protected void writeRecord(IARecordBuilder recordBuilder, DataOutput out, boolean writeTypeTag) throws IOException,
            AsterixException {
        if (recordBuilder.getFieldId(FeedConstants.StatisticsConstants.INTAKE_TIMESTAMP) > 0
                || recordBuilder.getFieldId(FeedConstants.StatisticsConstants.STORE_TIMESTAMP) > 0) {
            super.writeRecord(recordBuilder, out, writeTypeTag);
        } else {
            tupleDecorator.addTupleId(recordBuilder);
            tupleDecorator.addIntakePartition(recordBuilder);
            tupleDecorator.addIntakeTimestamp(recordBuilder);
            tupleDecorator.addStoreTimestamp(recordBuilder);
            super.writeRecord(recordBuilder, out, writeTypeTag);
        }
    }

}