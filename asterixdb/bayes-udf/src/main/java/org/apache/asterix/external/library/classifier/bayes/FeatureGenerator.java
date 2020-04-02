/**
 * Copyright (c) 2016 - 2018 Syncleus, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.external.library.classifier.bayes;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FeatureGenerator {
// Pattern for recognizing a URL, based off RFC 3986
    private static final Pattern urlPattern = Pattern.compile(
            "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
            + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
            + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
    private static final Pattern USERNAME_PATTERN = Pattern.compile("(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9-_]+)");

    public static String replaceUrl(String tweet){
        String modifiedTweet = tweet.replaceAll("https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)", "URL");
        return modifiedTweet;

        // Matcher matcher = urlPattern.matcher(tweet);
        // int matchStart = 0, matchEnd = 0;
        // String modifiedTweet = "";
        // try {
        //     while (matcher.find()) {
        //         matchStart = matcher.start(1);
        //         matchEnd = matcher.end();
        //     }

        //     if (matchEnd == 0){
        //         return tweet;
        //     }

        //     modifiedTweet = tweet.replaceAll(tweet.substring(matchStart, matchEnd), "URL");
        //     return modifiedTweet;
        // } catch(Exception e){
        //     return tweet;
        // }
    }         
    public static String replaceUsername(String tweet){
        // Matcher matcher = urlPattern.matcher(tweet);
        // int matchStart = 0, matchEnd = 0;
        // while (matcher.find()) {
        //     matchStart = matcher.start(1);
        //     matchEnd = matcher.end();
        // }

        // if (matchEnd == 0){
        //     return tweet;
        // }

        // String modifiedTweet = tweet.replaceAll(tweet.substring(matchStart, matchEnd), "URL");
        String modifiedTweet = tweet.replaceAll("(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@([A-Za-z]+[A-Za-z0-9-_]+)", "AT_USER");
        return modifiedTweet;
    }         

    public static String concat(String[] words, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++)
            sb.append((i > start ? " " : "") + words[i]);
        return sb.toString();
    }

    public static List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        String[] words = str.split(" ");
        for (int i = 0; i < words.length - n + 1; i++)
            ngrams.add(concat(words, i, i+n));
        return ngrams;
    }

    public static List<char[]> TweetToFeaturesChararray(String tweet){
        ArrayList<char[]> features = new ArrayList<char[]>();
        tweet = replaceUrl(tweet);
        tweet = replaceUsername(tweet);
        for (int i = 1; i <=3; i++){
            for (String ngram : ngrams(i, tweet)){
                features.add(ngram.toCharArray());
            }
        }
        return (List<char[]>) features;
    }
    public static List<String> TweetToFeatures(String tweet){
        ArrayList<String> features = new ArrayList<String>();
        tweet = replaceUrl(tweet);
        tweet = replaceUsername(tweet);
        for (int i = 1; i <=3; i++){
            for (String ngram : ngrams(i, tweet)){
                features.add(ngram);
            }
        }
        return (List<String>) features;
    }

    public static void main(String[] args){
        System.out.println(replaceUrl(replaceUsername("Decaf isn't cutting it right now.  @torstenbm ? http://blip.fm/~79g85")));
    }
}