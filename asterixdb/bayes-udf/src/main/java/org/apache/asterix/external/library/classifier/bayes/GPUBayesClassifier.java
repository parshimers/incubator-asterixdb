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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.aparapi.Kernel;
import com.aparapi.Range;

import org.apache.asterix.external.library.classifier.Classification;
import org.apache.asterix.external.library.classifier.Classifier;

public class GPUBayesClassifier<T, K> extends Classifier<T, K> {
    public char[] featureVector;
    public int[] featureIndexVector;
    public float[] probabilityMatrix;
    public char[][] unFlattenedFeatureVector;

    public void populateVectors() {
        this.unFlattenedFeatureVector = new char[this.totalFeatureCount.size()][];
        this.featureIndexVector = new int[this.totalFeatureCount.size()+1];
        this.probabilityMatrix = new float[this.totalFeatureCount.size()*2];
        
        int index = 0;
        Enumeration features = this.totalFeatureCount.keys();
        while (features.hasMoreElements()) {
            char[] feature = (char[]) features.nextElement();
            unFlattenedFeatureVector[index] = feature;

            // Store indexes of first char of feature to keep track of features
            // after flattening feature vector
            if (index == 0){
                this.featureIndexVector[index + 1] = feature.length;
            } else {
                this.featureIndexVector[index + 1] = this.featureIndexVector[index] + feature.length;
            }

            // Smoothing
            int featureCountInPositive = (this.featureCountPerCategory.get("positive").get(feature) == null) ? 1
                    : this.featureCountPerCategory.get("positive").get(feature);
            int featureCountInNegative = (this.featureCountPerCategory.get("negative").get(feature) == null) ? 1
                    : this.featureCountPerCategory.get("negative").get(feature);

            // Assigning probabilities to matrix
            this.probabilityMatrix[index*2] = (float) featureCountInPositive
                    / (float) totalFeatureCount.get(feature);
            this.probabilityMatrix[index*2+1] = (float) featureCountInNegative
                    / (float) totalFeatureCount.get(feature);

            // Index increment for each feature processed
            index++;
        }

        // Initialize flattened feature vector
        this.featureVector = flattenCharArray(this.unFlattenedFeatureVector);
        
    }

    public static int[] makeIndexArray(char[][] featureList){
        int[] indexArray = new int[featureList.length + 1];
        indexArray[0] = 0;
        for (int i = 0; i < featureList.length; i++){
            if (i == 0){
                indexArray[i + 1] = featureList[i].length;
            } else {
                indexArray[i + 1] = indexArray[i] + featureList[i].length;
            }
        }
        return indexArray;
    }

    public static char[] flattenCharArray(char[][] bumpyArray){
        int position = 0;
        int arraySize = size(bumpyArray);
        char[] flattenedArray = new char[arraySize];
        for (char[] feature : bumpyArray) {
            System.arraycopy(feature, 0, flattenedArray, position, feature.length);
            position += feature.length;
        }
        return flattenedArray;
    }

    public static int size(Object object) {
        if (!object.getClass().isArray()) {
            return 1;
        }
        int size = 0;
        for (int i = 0; i < Array.getLength(object); i++) {
            size += size(Array.get(object, i));
        }
        return size;
    }

    private float featuresProbabilityProduct(Collection<T> features, K category) {
        final char[][] featuresFromTweet = features.toArray(new char[0][]);
        final char[] tweetFeatureVector = flattenCharArray(featuresFromTweet);
        final int[] tweetFeatureIndexVector = makeIndexArray(featuresFromTweet);

        final int categoryIndex = (category.equals("positive")) ? 0 : 1;
        final float[] probabilityContributions = new float[featuresFromTweet.length];
        final char[] featureVector = this.featureVector;
        final int[] featureIndexVector = this.featureIndexVector;
        final float[] probabilityMatrix = this.probabilityMatrix;

        Kernel kernel = new Kernel() {
            @Override
            public void run() {

                int gid = getGlobalId();
                boolean couldBeMatch = true;
                boolean foundMatch = false;
                for (int i = 0; i < featureIndexVector.length-1 && !foundMatch; i++){
                    for (int c = tweetFeatureIndexVector[gid]; c < tweetFeatureIndexVector[gid + 1] && couldBeMatch; c++){
                        int relativeIndex = c - tweetFeatureIndexVector[gid];
                        if (tweetFeatureVector[c] != featureVector[featureIndexVector[i]+relativeIndex]){
                            couldBeMatch = false;
                        }
                    }
                    if (couldBeMatch){
                        probabilityContributions[gid] = probabilityMatrix[2*i + categoryIndex];
                        foundMatch = true;
                    }
                    
                    // Resetting before next term
                    couldBeMatch = true;
                }
                foundMatch = false;
            }
        };  
        
        Range range = Range.create(featuresFromTweet.length);
        kernel.execute(range);

        float probability = 1.0f;
        for (int i=0; i<probabilityContributions.length; i++){
            probability *= probabilityContributions[i];
        }
        return probability;
    }

    private float categoryProbability(Collection<T> features, K category) {
        return ((float) this.getCategoryCount(category)
                    / (float) this.getCategoriesTotal())
                * featuresProbabilityProduct(features, category);
    }


    private SortedSet<Classification<T, K>> categoryProbabilities(
            Collection<T> features) {

        SortedSet<Classification<T, K>> probabilities =
                new TreeSet<Classification<T, K>>(
                        new Comparator<Classification<T, K>>() {

                    public int compare(Classification<T, K> o1,
                            Classification<T, K> o2) {
                        int toReturn = Float.compare(
                                o1.getProbability(), o2.getProbability());
                        if ((toReturn == 0)
                                && !o1.getCategory().equals(o2.getCategory()))
                            toReturn = -1;
                        return toReturn;
                    }
                });

        for (K category : this.getCategories())
            probabilities.add(new Classification<T, K>(
                    features, category,
                    this.categoryProbability(features, category)));
        return probabilities;
    }


    @Override
    public Classification<T, K> classify(Collection<T> features) {
        SortedSet<Classification<T, K>> probabilites =
                this.categoryProbabilities(features);

        if (probabilites.size() > 0) {
            return probabilites.last();
        }
        return null;
    }
    
    public Collection<Classification<T, K>> classifyDetailed(
            Collection<T> features) {
        return this.categoryProbabilities(features);
    }

}
