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

public class HilbertCurve {
    public static final HilbertState[] STATE_DIAGRAM = new HilbertState[] {
            new HilbertState(new int[] { 3, 0, 1, 0 }, new int[] { 0, 1, 3, 2 }),
            new HilbertState(new int[] { 1, 1, 0, 2 }, new int[] { 2, 1, 3, 0 }),
            new HilbertState(new int[] { 2, 3, 2, 1 }, new int[] { 2, 3, 1, 0 }),
            new HilbertState(new int[] { 0, 2, 3, 3 }, new int[] { 0, 3, 1, 2 }) };
    
    //2d arrays are generated from HilbertValueConvertRunner.java
    public static final int[][] HILBERT_VALUE_DIMENSION2_ORDER2 = new int[][]  {
        {   0,  1,  14, 15  },
        {   3,  2,  13, 12  },
        {   4,  7,  8,  11  },
        {   5,  6,  9,  10  }
    };
    
    public static final int[][] HILBERT_VALUE_DIMENSION2_ORDER3 = new int[][]  {
        {   0,  3,  4,  5,  58, 59, 60, 63  },
        {   1,  2,  7,  6,  57, 56, 61, 62  },
        {   14, 13, 8,  9,  54, 55, 50, 49  },
        {   15, 12, 11, 10, 53, 52, 51, 48  },
        {   16, 17, 30, 31, 32, 33, 46, 47  },
        {   19, 18, 29, 28, 35, 34, 45, 44  },
        {   20, 23, 24, 27, 36, 39, 40, 43  },
        {   21, 22, 25, 26, 37, 38, 41, 42  }
    };
    
    public static final int[][] HILBERT_VALUE_DIMENSION2_ORDER4 = new int[][]  {
        {   0,  1,  14, 15, 16, 19, 20, 21, 234,    235,    236,    239,    240,    241,    254,    255 },
        {   3,  2,  13, 12, 17, 18, 23, 22, 233,    232,    237,    238,    243,    242,    253,    252 },
        {   4,  7,  8,  11, 30, 29, 24, 25, 230,    231,    226,    225,    244,    247,    248,    251 },
        {   5,  6,  9,  10, 31, 28, 27, 26, 229,    228,    227,    224,    245,    246,    249,    250 },
        {   58, 57, 54, 53, 32, 35, 36, 37, 218,    219,    220,    223,    202,    201,    198,    197 },
        {   59, 56, 55, 52, 33, 34, 39, 38, 217,    216,    221,    222,    203,    200,    199,    196 },
        {   60, 61, 50, 51, 46, 45, 40, 41, 214,    215,    210,    209,    204,    205,    194,    195 },
        {   63, 62, 49, 48, 47, 44, 43, 42, 213,    212,    211,    208,    207,    206,    193,    192 },
        {   64, 67, 68, 69, 122,    123,    124,    127,    128,    131,    132,    133,    186,    187,    188,    191 },
        {   65, 66, 71, 70, 121,    120,    125,    126,    129,    130,    135,    134,    185,    184,    189,    190 },
        {   78, 77, 72, 73, 118,    119,    114,    113,    142,    141,    136,    137,    182,    183,    178,    177 },
        {   79, 76, 75, 74, 117,    116,    115,    112,    143,    140,    139,    138,    181,    180,    179,    176 },
        {   80, 81, 94, 95, 96, 97, 110,    111,    144,    145,    158,    159,    160,    161,    174,    175 },
        {   83, 82, 93, 92, 99, 98, 109,    108,    147,    146,    157,    156,    163,    162,    173,    172 },
        {   84, 87, 88, 91, 100,    103,    104,    107,    148,    151,    152,    155,    164,    167,    168,    171 },
        {   85, 86, 89, 90, 101,    102,    105,    106,    149,    150,    153,    154,    165,    166,    169,    170 }
    };
    
    public static final int DIMENSION2_ORDER2_CELL_NUM = 16;
    public static final int DIMENSION2_ORDER3_CELL_NUM = 64;
    public static final int DIMENSION2_ORDER4_CELL_NUM = 256;
    public static final int DIMENSION2_ORDER2_AXIS_CELL_NUM = 4;
    public static final int DIMENSION2_ORDER3_AXIS_CELL_NUM = 8;
    public static final int DIMENSION2_ORDER4_AXIS_CELL_NUM = 16;
}
