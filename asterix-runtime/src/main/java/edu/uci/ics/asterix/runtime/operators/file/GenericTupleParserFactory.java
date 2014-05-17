package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class GenericTupleParserFactory implements ITupleParserFactory {

	private static final long serialVersionUID = 1L;

	private ARecordType recType;
	private final IDataParser dataParser;

	public GenericTupleParserFactory(IDataParser dataParser, ARecordType recType) {
		this.recType = recType;
		this.dataParser = dataParser;
	}

	@Override
	public ITupleParser createTupleParser(IHyracksTaskContext ctx)
			throws HyracksDataException {
		return new GenericTupleParser(ctx, recType, dataParser);
	}

	private static class GenericTupleParser extends AbstractTupleParser {

		private final IDataParser dataParser;

		public GenericTupleParser(IHyracksTaskContext ctx, ARecordType recType,
				IDataParser dataParser) throws HyracksDataException {
			super(ctx, recType);
			this.dataParser = dataParser;
		}

		@Override
		public IDataParser getDataParser() {
			return null;
		}

	}

}
