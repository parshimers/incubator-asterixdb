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

/**
 * Simple interface defining the method to calculate the feature probability.
 *
 * @author Philipp Nolte
 *
 * @param <T>
 *            The feature class.
 * @param <K>
 *            The category class.
 */
public interface IFeatureProbability<T, K> {

    /**
     * Returns the probability of a <code>feature</code> being classified as
     * <code>category</code> in the learning set.
     * 
     * @param feature
     *            the feature to return the probability for
     * @param category
     *            the category to check the feature against
     * @return the probability <code>p(feature|category)</code>
     */
    public float featureProbability(T feature, K category);

}
