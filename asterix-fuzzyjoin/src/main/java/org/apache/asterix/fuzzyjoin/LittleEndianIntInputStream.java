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

package org.apache.asterix.fuzzyjoin;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LittleEndianIntInputStream extends FilterInputStream {

    public LittleEndianIntInputStream(InputStream in) {
        super(in);
    }

    public int readInt() throws IOException {
        int a = read();
        if (a == -1) {
            throw new EOFException();
        }
        int b = read();
        if (b == -1) {
            throw new EOFException();
        }
        int c = read();
        if (c == -1) {
            throw new EOFException();
        }
        int d = read();
        if (d == -1) {
            throw new EOFException();
        }
        return (a | (b << 8) | (c << 16) | (d << 24)); // little endian
    }
}
