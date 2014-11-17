package edu.uci.ics.asterix.common.parse;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public interface IAsterixTupleParserFactory extends ITupleParserFactory {

    public void initialize(int partition);
}
