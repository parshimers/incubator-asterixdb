package org.apache.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class StreamBasedAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOGGER = Logger.getLogger(StreamBasedAdapter.class.getName());

    public abstract InputStream getInputStream(int partition) throws IOException;

    protected final ITupleParser tupleParser;

    protected final IAType sourceDatatype;

    public StreamBasedAdapter(ITupleParserFactory parserFactory, IAType sourceDatatype, IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        this.tupleParser = parserFactory.createTupleParser(ctx);
        this.sourceDatatype = sourceDatatype;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        InputStream in = getInputStream(partition);
        if (in != null) {
            tupleParser.parse(in, writer);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not obtain input stream for parsing from adapter " + this + "[" + partition + "]");
            }
        }
    }

}
