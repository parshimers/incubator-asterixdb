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
package org.apache.asterix.external.library.classifier.example;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Map;

import com.opencsv.CSVReader;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.apache.asterix.external.library.classifier.bayes.BayesClassifier;
import org.apache.asterix.external.library.classifier.bayes.FeatureGenerator;
import org.apache.asterix.external.library.classifier.Classifier;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.Math;
import java.nio.file.Files;

public class CPUExample {

    public static Reader getReader(String relativePath) throws UnsupportedEncodingException, FileNotFoundException {
        return new InputStreamReader(new FileInputStream(relativePath), "UTF-8");
    }

    public static void main(String[] args) throws InterruptedException {

        final Classifier<String, String> bayes = new BayesClassifier<String, String>();

        bayes.setMemoryCapacity(4000000);

        String testingCsvFile = "/Users/torsten/Java-Naive-Bayes-Classifier/example/testdata.manual.csv";
        String trainingCsvFile = "/Users/torsten/Java-Naive-Bayes-Classifier/example/training.shuffled.csv";
        BufferedReader br = null;
        String cvsSplitBy = ",";
        long startTime, endTime;
        FeatureGenerator FeatureGenerator = new FeatureGenerator();
        int trainingTotal = 0, learnedPositives = 0, learnedNegatives = 0;
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
                    System.out.println(tweet[5]);
                    break;
                }
                tweet[0] = tweet[0].replaceAll("\"", "");
                tweet[5] = tweet[5].replaceAll("\"", "");

                if (tweet[0].equals("4")) {
                    // System.out.println("Learned positive , tweet=" + tweet[5] + "]");
                    bayes.learn("positive", FeatureGenerator.TweetToFeatures(tweet[5]));
                    learnedPositives++;
                } else if (tweet[0].equals("0")) {
                    // System.out.println("Learned negative , tweet=" + tweet[5] + "]");
                    bayes.learn("negative", FeatureGenerator.TweetToFeatures(tweet[5]));
                    learnedNegatives++;
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

        System.out.println(
                "Training completed in " + String.valueOf((endTime - startTime) / Math.pow(10, 9)) + " seconds");
        System.out.println(learnedPositives);
        System.out.println(learnedNegatives);
        System.out.println(trainingTotal);

        String category; 
        int rights = 0, wrongs = 0;
        startTime = System.nanoTime();
        int totals = 0;

        try {
            CsvParserSettings settings = new CsvParserSettings();
            CsvParser parser = new CsvParser(settings);
            Reader reader = getReader(trainingCsvFile);
            parser.beginParsing(reader);

            String[] tweet;
            while ((tweet = parser.parseNext()) != null) {	
                totals++;
                if (totals <= 1000000){
                    continue;
                }
                if (totals > 1010000){
                    break;
                }

                tweet[0] = tweet[0].replaceAll("\"", "");
                tweet[5] = tweet[5].replaceAll("\"", "");

                if (tweet[0].equals("2")){
                    continue;
                }
                category = bayes.classify(FeatureGenerator.TweetToFeatures(tweet[5])).getCategory();
                
                if ( (category.equals("negative") && tweet[0].equals("0")) || (category.equals("positive") && tweet[0].equals("4")) ){
                    rights++;
                } else {
                    // System.out.println(String.format("Predicted %s but was %s", category, tweet[0]));
                    wrongs++;
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

        System.out.println("Classification completed in " + String.valueOf((endTime-startTime)/Math.pow(10, 9)) + " seconds");
        System.out.println(String.format("With %d correct predictions and %d wrong", rights, wrongs));
        System.out.println(totals);
    }
}
