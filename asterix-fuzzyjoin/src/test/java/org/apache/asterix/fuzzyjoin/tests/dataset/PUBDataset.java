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
 * Author: Rares Vernica <rares (at) apache.org>
 */

package org.apache.asterix.fuzzyjoin.tests.dataset;

public class PUBDataset extends PublicationsDataset {
    private static final String DBLP_SUFFIX = "dblp";
    private static final String CSX_SUFFIX = "csx";
    private static final String NAME = "pub";
    private static final int NO_RECORDS = 1385532;
    private static final float THRESHOLD = .8f;
    private static final String RECORD_DATA = "2,3";

    public PUBDataset() {
        super(NAME, NO_RECORDS, THRESHOLD, RECORD_DATA, DBLP_SUFFIX, CSX_SUFFIX);
    }

    public PUBDataset(float threshold) {
        super(NAME, NO_RECORDS, threshold, RECORD_DATA, DBLP_SUFFIX, CSX_SUFFIX);
    }

    public PUBDataset(float threshold, String recordData) {
        super(NAME, NO_RECORDS, threshold, recordData, DBLP_SUFFIX, CSX_SUFFIX);
    }
}
