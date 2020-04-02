/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.library;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.classifier.CPUBayesForSentimentAnalysis;
import org.apache.asterix.external.library.java.base.JLong;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;

public class CPUBayesFunction implements IExternalScalarFunction {

    private JString sentiment;
    public CPUBayesForSentimentAnalysis BayesClasifier;

    private long batchTime;
    private Integer classifiedDuringSecond = 0;

    private Integer seconds;

    @Override
    public void deinitialize() {}

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        // Read input record
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);

        JLong id = (JLong) inputRecord.getValueByName("id");
        JString text = (JString) inputRecord.getValueByName("text");

        // Populate result record
        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("id", id);
        result.setField("text", text);

        if (System.nanoTime() - this.batchTime > 1000000000){
            logClassifiedInBatchString();
            this.batchTime = System.nanoTime();
            this.seconds++;
        }

        sentiment.setValue(this.BayesClasifier.classify(text));
        result.setField("Sentiment", sentiment);
        functionHelper.setResult(result);
        
        this.classifiedDuringSecond++;
    }

    public void logClassifiedInBatchString(){
        System.out.println("Classified during second " + this.seconds.toString() + ": " + this.classifiedDuringSecond.toString());
        this.classifiedDuringSecond = 0;
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception{
        sentiment = new JString("");
        this.BayesClasifier = new CPUBayesForSentimentAnalysis();
        System.out.println("Initialization of Bayes Classifier started");
        this.BayesClasifier.trainModel();
        System.out.println("Bayes Classifier Initialized");
        this.batchTime = System.nanoTime();
        this.seconds = 0;
    }
}