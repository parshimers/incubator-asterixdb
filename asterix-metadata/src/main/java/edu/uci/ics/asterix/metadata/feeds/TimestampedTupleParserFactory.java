package edu.uci.ics.asterix.metadata.feeds;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
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

    private static final String INTAKE_TIMESTAMP = "intake-timestamp";
    private static final String COMPUTE_TIMESTAMP = "compute-timestamp";
    private static final String STORE_TIMESTAMP = "store-timestamp";

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

}
