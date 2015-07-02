package org.apache.asterix.common.parse;

import java.util.Map;

import org.apache.hyracks.dataflow.std.file.ITupleParser;

public interface IAsterixTupleParser extends ITupleParser{

    public void configure(Map<String, String> configuration);
    
}
