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

package org.apache.asterix.runtime.linearizer;

import org.apache.asterix.dataflow.data.nontagged.comparators.HilbertValueInt64Converter;
import org.apache.hyracks.storage.am.common.api.IHilbertValueComputer;

/* 
 * This class converts a given point represented as x/y coordinates into a 64-bit Hilbert value. 
 * The computation follows the following steps:
 * 1. check whether a given point is in geo-space(-180.0 ~ 180.0 for x and -90.0 ~ 90.0 for y). 
 *    If not, adjust the point to be in the space.
 * 2. convert the point into integer x/y coordinates where both x and y double values are mapped to integer values lies in from 0 to 2^31.
 *    where ,for example, a distance between 1 and 2 along x-axis represents 2cm approximately in geo-space.
 * 3. convert the integer x/y coordinates into a Hilbert value using HilbertValueInt64Converter.
 * 
 * done!  
 */
public class GeoCoordinates2HilbertValueConverter implements IHilbertValueComputer {

    private final static int HILBERT_SPACE_ORDER = 31;
    private static final int MAX_COORDINATE = 180;
    private static final long COORDINATE_EXTENSION_FACTOR = (long) (((double) (1L << HILBERT_SPACE_ORDER)) / (2 * MAX_COORDINATE));
    private final HilbertValueInt64Converter hilbertConverter = new HilbertValueInt64Converter(2);

    @Override
    public long computeInt64HilbertValue(double x, double y) {
        //1. check whether a given point is in geo-space(-180.0 ~ 180.0 for x and -90.0 ~ 90.0 for y).
        //   If not, adjust the point to be in the space.
        if (x < -180.0) {
            x = -180.0;
        } else if (x >= 180.0) {
            x = 179.0;
        }
        if (y < -90.0) {
            y = -90.0;
        } else if (y >= 90.0) {
            y = 89.0;
        }

        //2. convert the point into integer x/y coordinates where both x and y double values are mapped to integer values lies in from 0 to 2^31.
        //   where ,for example, a distance between 1 and 2 along x-axis represents 2cm approximately in geo-space.
        long lx = getLongCoordinate(x);
        long ly = getLongCoordinate(y);

        //3. convert the integer x/y coordinates into a Hilbert value using HilbertValueInt64Converter.
        long hilbertValue = hilbertConverter.convert(HILBERT_SPACE_ORDER, lx, ly);

        return hilbertValue;
    }

    private long getLongCoordinate(double c) {
        return ((long) ((c + (double) MAX_COORDINATE) * (double) COORDINATE_EXTENSION_FACTOR));
    }

}
