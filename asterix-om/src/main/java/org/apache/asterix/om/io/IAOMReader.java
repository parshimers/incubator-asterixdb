/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.om.io;

import org.apache.asterix.om.base.IAObject;

public interface IAOMReader {

    // Initializes a reader for the collection defined by a certain location.
    public void init(IALocation location) throws AsterixIOException;

    // Reads the current object and goes to the next item in the collection.
    // When it reaches the end, it returns null.
    public IAObject read() throws AsterixIOException;

    public void close() throws AsterixIOException;
}
