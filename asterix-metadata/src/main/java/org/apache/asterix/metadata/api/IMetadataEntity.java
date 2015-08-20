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

package edu.uci.ics.asterix.metadata.api;

import java.io.Serializable;

import edu.uci.ics.asterix.metadata.MetadataCache;

public interface IMetadataEntity extends Serializable {

    public static final int PENDING_NO_OP = 0;
    public static final int PENDING_ADD_OP = 1;
    public static final int PENDING_DROP_OP = 2;

    Object addToCache(MetadataCache cache);

    Object dropFromCache(MetadataCache cache);
}
