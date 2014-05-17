package edu.uci.ics.asterix.tools.external.data;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.feeds.FeedConstants.StatisticsConstants;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class TimestampedTupleParserFactory implements ITupleParserFactory {

    private static final long serialVersionUID = 1L;

    private final ARecordType recordType;

    public TimestampedTupleParserFactory(ARecordType recordType) {
        this.recordType = recordType;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        return new TimestampedTupleParser(ctx, recordType);
    }

    private static class TimestampedTupleParser extends AbstractTupleParser {

        public TimestampedTupleParser(IHyracksTaskContext ctx, ARecordType recType) throws HyracksDataException {
            super(ctx, recType);
        }

        @Override
        public IDataParser getDataParser() {
            return new TimestampedADMDataParser();
        }

    }

    private static class TimestampedADMDataParser extends ADMDataParser {

        protected void writeRecord(IARecordBuilder recordBuilder, DataOutput out, boolean writeTypeTag)
                throws IOException, AsterixException {
            if (recordBuilder.getFieldId(StatisticsConstants.INTAKE_TIMESTAMP) > 0
                    || recordBuilder.getFieldId(StatisticsConstants.COMPUTE_TIMESTAMP) > 0
                    || recordBuilder.getFieldId(StatisticsConstants.STORE_TIMESTAMP) > 0) {
                super.writeRecord(recordBuilder, out, writeTypeTag);
            } else {
                addAttribute(StatisticsConstants.INTAKE_TIMESTAMP, System.currentTimeMillis(), recordBuilder);
                addAttribute(StatisticsConstants.COMPUTE_TIMESTAMP, 0, recordBuilder);
                addAttribute(StatisticsConstants.STORE_TIMESTAMP, 0, recordBuilder);
                recordBuilder.write(out, true);
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

}
