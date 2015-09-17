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

package org.apache.asterix.dataflow.data.nontagged.comparators;

/*
 * This compares two points based on the hilbert curve. Currently, it only supports
 * doubles (this can be changed by changing all doubles to ints as there are no
 * number generics in Java) in the two-dimensional space. For more dimensions, the
 * state machine has to be automatically generated. The idea of the fractal generation
 * of the curve is described e.g. in http://dl.acm.org/ft_gateway.cfm?id=383528&type=pdf
 * 
 * Unlike the described approach, this comparator does not compute the hilbert value at 
 * any point. Instead, it only evaluates how the two inputs compare to each other. This
 * is done by starting at the lowest hilbert resolution and zooming in on the fractal until
 * the two points are in different quadrants.
 * 
 * As a performance optimization, the state of the state machine is saved in a stack and 
 * maintained over comparisons. The idea behind this is that comparisons are usually in a
 * similar area (e.g. geo coordinates). Zooming in from [-MAX_VALUE, MAX_VALUE] would take
 * ~300 steps every time. Instead, the comparator start from the previous state and zooms out
 * if necessary
 */

public class HilbertValueConverter {
    public static final int[] intArr = new int[] { 1, 2, 3 };

    private final HilbertState[] diagram = HilbertCurve.STATE_DIAGRAM;

    public HilbertValueConverter(int dimension) {
        if (dimension != 2)
            throw new IllegalArgumentException();
    }

    public int convert(int order, int x, int y) {
        // Order of Hilbert curve such as 2, 3, 4. 
        // Order = 2 -> cell number: 2^2 x 2^2 
        // Order = 3 -> cell number: 2^3 x 2^3
        // and so on.
        assert ((int) Math.pow(2, order) >= x) && ((int) Math.pow(2, order) >= y);

        int coordinate = 0x0000;
        int hVal = 0x0000;
        int state = 0;

        for (int i = order - 1; i >= 0; i--) {
            //compute coordinate by concatenating msb of x and msb of y  
            coordinate = 0x0000;
            if ((x & (1 << i)) != 0)
                coordinate = 0x0002;
            if ((y & (1 << i)) != 0)
                coordinate |= 0x0001;

            //get Hilbert value for the computed coordinate based on HilbertState diagram
            hVal |= diagram[state].sn[coordinate] << i * 2;

            //update state
            state = diagram[state].nextState[coordinate];
        }

        return hVal;
    }
}
