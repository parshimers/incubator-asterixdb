/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;

import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Provides the functionality of fetching data in form of ADM records from a Hive dataset.
 */
public class HiveAdapter extends FileSystemBasedAdapter {

    private static final long serialVersionUID = 1L;

    private HDFSAdapter hdfsAdapter;

    public HiveAdapter(IAType atype, HDFSAdapter hdfsAdapter, ITupleParserFactory parserFactory, IHyracksTaskContext ctx)
            throws HyracksDataException {
        super(parserFactory, atype, ctx);
        this.hdfsAdapter = hdfsAdapter;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return hdfsAdapter.getInputStream(partition);
    }

}
