/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.recordgroup;

import edu.uci.ics.asterix.fuzzyjoin.similarity.SimilarityFilters;

public abstract class RecordGroup {
    protected final int noGroups;
    protected final SimilarityFilters fuzzyFilters;

    public RecordGroup(int noGroups, SimilarityFilters fuzzyFilters) {
        this.noGroups = noGroups;
        this.fuzzyFilters = fuzzyFilters;
    }

    public abstract Iterable<Integer> getGroups(Integer token, Integer length);

    public abstract boolean isLengthOnly();
}
