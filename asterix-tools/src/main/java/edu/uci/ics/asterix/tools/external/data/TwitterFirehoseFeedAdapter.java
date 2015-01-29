/*
x * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.dataset.adapter.StreamBasedAdapter;
import edu.uci.ics.asterix.metadata.feeds.IFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * TPS can be configured between 1 and 20,000
 * 
 * @author ramang
 */
public class TwitterFirehoseFeedAdapter extends StreamBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = Logger.getLogger(TwitterFirehoseFeedAdapter.class.getName());

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private PipedOutputStream outputStream = new PipedOutputStream();

    private PipedInputStream inputStream = new PipedInputStream(outputStream);

    private final TwitterServer twitterServer;

    public TwitterFirehoseFeedAdapter(Map<String, String> configuration, ITupleParserFactory parserFactory,
            ARecordType outputtype, int partition, IHyracksTaskContext ctx) throws Exception {
        super(parserFactory, outputtype, ctx);
        this.twitterServer = new TwitterServer(configuration, partition, outputtype, outputStream, executorService);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        twitterServer.start();
        super.start(partition, writer);
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return inputStream;
    }

    public static class TwitterServer {
        private final DataProvider dataProvider;
        private final ExecutorService executorService;

        public TwitterServer(Map<String, String> configuration, int partition, ARecordType outputtype, OutputStream os,
                ExecutorService executorService) throws Exception {
            dataProvider = new DataProvider(configuration, outputtype, partition, os);
            this.executorService = executorService;
        }

        public void stop() throws IOException {
            dataProvider.stop();
        }

        public void start() {
            executorService.execute(dataProvider);
        }

    }

    public static class DataProvider implements Runnable {

        public static final String KEY_MODE = "mode";

        private TweetGenerator tweetGenerator;
        private boolean continuePush = true;
        private int batchSize;
        private final Mode mode;
        private final OutputStream os;

        public static enum Mode {
            AGGRESSIVE,
            CONTROLLED
        }

        public DataProvider(Map<String, String> configuration, ARecordType outputtype, int partition, OutputStream os)
                throws Exception {
            this.tweetGenerator = new TweetGenerator(configuration, partition, TweetGenerator.OUTPUT_FORMAT_ADM_STRING,
                    os);
            this.os = os;
            mode = configuration.get(KEY_MODE) != null ? Mode.valueOf(configuration.get(KEY_MODE).toUpperCase())
                    : Mode.AGGRESSIVE;
            switch (mode) {
                case CONTROLLED:
                    String tpsValue = configuration.get(TweetGenerator.KEY_TPS);
                    if (tpsValue == null) {
                        throw new IllegalArgumentException("TPS value not configured. use tps=<value>");
                    }
                    batchSize = Integer.parseInt(tpsValue);
                    break;
                case AGGRESSIVE:
                    batchSize = 5000;
                    break;
            }
        }

        @Override
        public void run() {
            boolean moreData = true;
            long startBatch;
            long endBatch;

            try {
                while (moreData && continuePush) {
                    switch (mode) {
                        case AGGRESSIVE:
                            moreData = tweetGenerator.setNextRecordBatch(batchSize);
                            break;
                        case CONTROLLED:
                            startBatch = System.currentTimeMillis();
                            moreData = tweetGenerator.setNextRecordBatch(batchSize);
                            endBatch = System.currentTimeMillis();
                            if (endBatch - startBatch < 1000) {
                                Thread.sleep(1000 - (endBatch - startBatch));
                            } else {
                                if (LOGGER.isLoggable(Level.WARNING)) {
                                    LOGGER.warning("Unable to reach the required tps of " + batchSize);
                                }
                            }
                            break;
                    }
                }
                os.close();
            } catch (Exception e) {
                if (LOGGER.isLoggable(Level.WARNING)) {
                    LOGGER.warning("Exception in adapter " + e.getMessage());
                }
            }
        }

        public void stop() {
            continuePush = false;
        }

    }

    @Override
    public void stop() throws Exception {
        twitterServer.stop();
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PUSH;
    }

}