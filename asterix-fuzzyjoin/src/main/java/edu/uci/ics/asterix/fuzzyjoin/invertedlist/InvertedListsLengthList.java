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

package edu.uci.ics.asterix.fuzzyjoin.invertedlist;

import java.util.HashMap;

public class InvertedListsLengthList implements InvertedLists {
    public final HashMap<Integer, InvertedListLengthList> invertedLists;

    public InvertedListsLengthList() {
        invertedLists = new HashMap<Integer, InvertedListLengthList>();
    }

    public InvertedListLengthList get(int token) {
        return invertedLists.get(token);
    }

    public void index(int token, int[] element) {
        InvertedListLengthList list = invertedLists.get(token);
        if (list == null) {
            list = new InvertedListLengthList();
            invertedLists.put(token, list);
        }
        list.add(element);
    }

    public void prune(int minLength) {
        for (InvertedListLengthList l : invertedLists.values()) {
            l.setMinLength(minLength);
        }
    }
}
