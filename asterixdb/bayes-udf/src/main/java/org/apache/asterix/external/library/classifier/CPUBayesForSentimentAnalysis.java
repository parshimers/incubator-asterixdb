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
package org.apache.asterix.external.library.classifier;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.asterix.external.library.classifier.bayes.BayesClassifier;
import org.apache.asterix.external.library.classifier.bayes.FeatureGenerator;
import org.apache.asterix.external.library.java.base.JString;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.Math;

public class CPUBayesForSentimentAnalysis {

    private Classifier<String, String> bayes;

    public CPUBayesForSentimentAnalysis() {
        this.bayes = new BayesClassifier<String, String>();
        this.bayes.setMemoryCapacity(4000000);
    }

    public static Reader getReader(String relativePath) throws UnsupportedEncodingException, FileNotFoundException {
        return new InputStreamReader(new FileInputStream(relativePath), "UTF-8");
    }

    public void trainModel(){
        // Replace with path to your own copy of the sentiment140 training data
        // http://help.sentiment140.com/for-students
        String trainingCsvFile = "/Users/torsten/Java-Naive-Bayes-Classifier/example/training.shuffled.csv";
        
        BufferedReader br = null;
        long startTime, endTime;
        int trainingTotal = 0;
        startTime = System.nanoTime();
        try {
            CsvParserSettings settings = new CsvParserSettings();
            CsvParser parser = new CsvParser(settings);
            Reader reader = getReader(trainingCsvFile);
            parser.beginParsing(reader);

            String[] tweet;
            while ((tweet = parser.parseNext()) != null) {
                trainingTotal++;
                if (trainingTotal > 1000000) {
                    break;
                }
                String tweetSentiment = tweet[0].replaceAll("\"", "");
                String tweetText = tweet[5].replaceAll("\"", "");

                if (tweetSentiment.equals("4")) {
                    this.bayes.learn("positive", FeatureGenerator.TweetToFeatures(tweetText));
                } else if (tweetSentiment.equals("0")) {
                    this.bayes.learn("negative", FeatureGenerator.TweetToFeatures(tweetText));
                }
            }
            parser.stopParsing();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        endTime = System.nanoTime();

        System.out.println("Training completed in " + String.valueOf((endTime - startTime) / Math.pow(10, 9)) + " seconds");
    }

    public String classify(JString asterixTweet) {
        String at = asterixTweet.getValue();
        return this.bayes.classify(FeatureGenerator.TweetToFeatures(at)).getCategory();
    }
}
