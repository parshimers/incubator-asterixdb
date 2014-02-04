package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public class ExperimentAdmSchemafullRecordParserFactory extends AdmSchemafullRecordParserFactory {
    private static final long serialVersionUID = 1L;

    private final long duration;

    public ExperimentAdmSchemafullRecordParserFactory(ARecordType recType, long duration) {
        super(recType);
        this.duration = duration;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) throws HyracksDataException {
        return new ExperimentAdmTupleParser(ctx, recType, duration);
    }
}
