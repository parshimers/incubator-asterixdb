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
package edu.uci.ics.asterix.external.library.adaptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class TweetGenAdaptor extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private final String host;

    private final int port;

    private Socket socket;

    public TweetGenAdaptor(ITupleParserFactory parserFactory, ARecordType sourceDatatype,
            IHyracksTaskContext ctx, Map<String, String> configuration, int partition) throws IOException {
        super(parserFactory, sourceDatatype, ctx, partition);
        String hostArray = configuration.get(TweetGenAdaptorFactory.TWIITER_SERVER_HOST);
        this.host = hostArray.split(",")[partition];
        this.port = Integer.parseInt(configuration.get(TweetGenAdaptorFactory.TWIITER_SERVER_PORT));
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        socket = new Socket(host, port);
        return socket.getInputStream();
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

    @Override
    public void stop() throws Exception {
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

}
