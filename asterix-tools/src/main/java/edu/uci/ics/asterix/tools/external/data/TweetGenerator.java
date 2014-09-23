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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessageIterator;

public class TweetGenerator {

    private static Logger LOGGER = Logger.getLogger(TweetGenerator.class.getName());

    public static final String KEY_DURATION = "duration";
    public static final String KEY_TPS = "tps";
    public static final String KEY_VERBOSE = "verbose";
    public static final String KEY_FIELDS = "fields";

    public static final int INFINITY = 0;
    private static final int DEFAULT_DURATION = INFINITY;

    private int duration;
    private TweetMessageIterator tweetIterator = null;
    private int partition;
    private long tweetCount = 0;
    private int frameTweetCount = 0;
    private int numFlushedTweets = 0;
    private OutputStream os;
    private DataGenerator dataGenerator = null;
    private ByteBuffer outputBuffer = ByteBuffer.allocate(32 * 1024);
    private String[] fields;
    private boolean verbose;
    private long timestamp;

    public long getTweetCount() {
        return tweetCount;
    }

    public TweetGenerator(Map<String, String> configuration, int partition, OutputStream os) throws Exception {
        this.partition = partition;
        String value = configuration.get(KEY_DURATION);
        this.duration = value != null ? Integer.parseInt(value) : DEFAULT_DURATION;
        dataGenerator = new DataGenerator(new InitializationInfo());
        tweetIterator = dataGenerator.new TweetMessageIterator(duration);
        this.os = os;
        this.verbose = configuration.get(KEY_VERBOSE) != null ? Boolean.parseBoolean(configuration.get(KEY_VERBOSE))
                : false;
        this.fields = configuration.get(KEY_FIELDS) != null ? configuration.get(KEY_FIELDS).split(",") : null;
        this.timestamp = System.currentTimeMillis();
    }

    private void writeTweetString(TweetMessage tweetMessage) throws IOException {
        System.out.println("Writing tweet  " + tweetMessage);
        String tweet = fields == null ? tweetMessage + "\n" : tweetMessage.getAdmEquivalent(fields) + "\n";
        tweetCount++;
        if (verbose && tweetCount % 1000 == 0) {
            long timeElapsed = ((System.currentTimeMillis() - timestamp) / 1000L);
            System.out.println("Sent (so far) " + tweetCount + " last 1000 tweets in " + timeElapsed);
            timestamp = System.currentTimeMillis();
        }
        byte[] b = tweet.getBytes();
        if (outputBuffer.position() + b.length > outputBuffer.limit()) {
            flush();
            numFlushedTweets += frameTweetCount;
            frameTweetCount = 0;
            outputBuffer.put(b);
        } else {
            outputBuffer.put(b);
        }
        frameTweetCount++;
    }

    public int getNumFlushedTweets() {
        return numFlushedTweets;
    }

    private void flush() throws IOException {
        outputBuffer.flip();
        os.write(outputBuffer.array(), 0, outputBuffer.limit());
        outputBuffer.position(0);
        outputBuffer.limit(32 * 1024);
    }

    public boolean setNextRecordBatch(int numTweetsInBatch) throws Exception {
        boolean moreData = tweetIterator.hasNext();
        if (!moreData) {
            if (outputBuffer.position() > 0) {
                flush();
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Reached end of batch. Tweet Count: [" + partition + "]" + tweetCount);
            }
            return false;
        } else {
            int count = 0;
            while (count < numTweetsInBatch) {
                writeTweetString(tweetIterator.next());
                count++;
            }
            return true;
        }
    }

    public void resetOs(OutputStream os) {
        this.os = os;
    }
}