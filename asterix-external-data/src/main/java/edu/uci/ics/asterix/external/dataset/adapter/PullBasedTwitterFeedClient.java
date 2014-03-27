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

import java.util.List;
import java.util.Map;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.external.adapter.factory.PullBasedTwitterAdapterFactory;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

/**
 * An implementation of @see {PullBasedFeedClient} for the Twitter service. The
 * feed client fetches data from Twitter service by sending request at regular
 * (configurable) interval.
 */
public class PullBasedTwitterFeedClient extends PullBasedFeedClient {

    private String keywords;
    private Query query;
    private Twitter twitter;
    private int requestInterval = 10; // seconds
    private QueryResult result;

    private IAObject[] mutableTweetFields;
    private IAObject[] mutableUserFields;

    private ARecordType recordType;
    private int nextTweetIndex = 0;
    private AMutableRecord mutableUser;
    private long lastTweetIdReceived = 0;

    public PullBasedTwitterFeedClient(IHyracksTaskContext ctx, ARecordType recordType, PullBasedTwitterAdapter adapter) {
        twitter = getTwitterService();

        mutableUserFields = new IAObject[] { new AMutableString(null), new AMutableString(null), new AMutableInt32(0),
                new AMutableInt32(0), new AMutableString(null), new AMutableInt32(0) };
        mutableUser = new AMutableRecord((ARecordType) recordType.getFieldTypes()[1], mutableUserFields);

        mutableTweetFields = new IAObject[] { new AMutableString(null), mutableUser, new AMutableDouble(0),
                new AMutableDouble(0), new AMutableString(null), new AMutableString(null) };
        this.recordType = recordType;
        recordSerDe = new ARecordSerializerDeserializer(recordType);
        mutableRecord = new AMutableRecord(recordType, mutableTweetFields);
        initialize(adapter.getConfiguration());
    }

    private static Twitter getTwitterService() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("ecE3VLu7vnHa5y5A0rP8g");
        cb.setOAuthConsumerSecret("6ToSX51jDlgb7Nj46rMjLNLAvM84j9dqowPASp1d8Y");
        cb.setOAuthAccessToken("179203714-XBQ3sz01CIHbAXZGgcR4eW3dbV3JxXT5qXacl8n0");
        cb.setOAuthAccessTokenSecret("kvoGk0qBRzcuNUawicbABK6Bwt2SkKfDSB7RFYxGidsxh");
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }

    public ARecordType getRecordType() {
        return recordType;
    }

    public AMutableRecord getMutableRecord() {
        return mutableRecord;
    }

    @Override
    public InflowState setNextRecord() throws Exception {
        Status tweet;
        tweet = getNextTweet();
        if (tweet == null) {
            return InflowState.DATA_NOT_AVAILABLE;
        }

        User user = tweet.getUser();
        ((AMutableString) mutableUserFields[0]).setValue(user.getScreenName());
        ((AMutableString) mutableUserFields[1]).setValue(user.getLang());
        ((AMutableInt32) mutableUserFields[2]).setValue(user.getFriendsCount());
        ((AMutableInt32) mutableUserFields[3]).setValue(user.getStatusesCount());
        ((AMutableString) mutableUserFields[4]).setValue(user.getName());
        ((AMutableInt32) mutableUserFields[5]).setValue(user.getFollowersCount());

        ((AMutableString) mutableTweetFields[0]).setValue(tweet.getId() + "");

        for (int i = 0; i < 6; i++) {
            ((AMutableRecord) mutableTweetFields[1]).setValueAtPos(i, mutableUserFields[i]);
        }
        if (tweet.getGeoLocation() != null) {
            ((AMutableDouble) mutableTweetFields[2]).setValue(tweet.getGeoLocation().getLatitude());
            ((AMutableDouble) mutableTweetFields[3]).setValue(tweet.getGeoLocation().getLongitude());
        } else {
            ((AMutableDouble) mutableTweetFields[2]).setValue(0);
            ((AMutableDouble) mutableTweetFields[3]).setValue(0);
        }
        ((AMutableString) mutableTweetFields[4]).setValue(tweet.getCreatedAt().toString());
        ((AMutableString) mutableTweetFields[5]).setValue(tweet.getText());

        for (int i = 0; i < 6; i++) {
            mutableRecord.setValueAtPos(i, mutableTweetFields[i]);
        }

        System.out.println("Tweet:" + mutableRecord);
        return InflowState.DATA_AVAILABLE;
    }

    private void initialize(Map<String, String> params) {
        this.keywords = (String) params.get(PullBasedTwitterAdapterFactory.QUERY);
        this.requestInterval = Integer.parseInt((String) params.get(PullBasedTwitterAdapterFactory.INTERVAL));
        this.query = new Query(keywords);
        this.query.setCount(100);
    }

    private Status getNextTweet() throws TwitterException, InterruptedException {
        if (result == null || nextTweetIndex >= result.getTweets().size()) {
            Thread.sleep(1000 * requestInterval);
            query.setSinceId(lastTweetIdReceived);
            result = twitter.search(query);
            nextTweetIndex = 0;
        }
        if (result != null && !result.getTweets().isEmpty()) {
            List<Status> tw = result.getTweets();
            Status tweet = tw.get(nextTweetIndex++);
            if (lastTweetIdReceived < tweet.getId()) {
                lastTweetIdReceived = tweet.getId();
            }
            return tweet;
        } else {
            return null;
        }
    }

}
