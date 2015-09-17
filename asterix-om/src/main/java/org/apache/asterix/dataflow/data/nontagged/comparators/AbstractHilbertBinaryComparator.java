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

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.storage.am.btree.impls.HilbertBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.common.ophelpers.DoubleArrayList;
import org.apache.hyracks.storage.am.common.ophelpers.IntArrayList;

public abstract class AbstractHilbertBinaryComparator implements IBinaryComparator {

    //Currently, only 2 dimensional data is supported for Hilbert comparator.
    public static final int SUPPORTED_HILBERT_CURVE_DIMENSION = 2;
    
    protected final int dim = SUPPORTED_HILBERT_CURVE_DIMENSION;
    private final HilbertState[] states = new HilbertState[] {
            new HilbertState(new int[] { 3, 0, 1, 0 }, new int[] { 0, 1, 3, 2 }),
            new HilbertState(new int[] { 1, 1, 0, 2 }, new int[] { 2, 1, 3, 0 }),
            new HilbertState(new int[] { 2, 3, 2, 1 }, new int[] { 2, 3, 1, 0 }),
            new HilbertState(new int[] { 0, 2, 3, 3 }, new int[] { 0, 3, 1, 2 }) };

    private static final double INITIAL_STEP_SIZE = HilbertBTreeRangeSearchCursor.MAX_COORDINATE / 2;
    private double[] bounds = new double[dim];
    private double stepsize;
    private int state = 0;
    private IntArrayList stateStack = new IntArrayList(1100, 100);
    private DoubleArrayList boundsStack = new DoubleArrayList(2200, 200);
    protected double[] a = new double[dim];
    protected double[] b = new double[dim];
    public int count = 0;

    class HilbertState {
        public final int[] nextState;
        public final int[] position;

        public HilbertState(int[] nextState, int[] order) {
            this.nextState = nextState;
            this.position = order;
        }
    }

    private void resetStateMachine() {
        state = 0;
        stateStack.clear();
        stepsize = INITIAL_STEP_SIZE;
        bounds[0] = 0.0;
        bounds[1] = 0.0;
        boundsStack.clear();
    }

    public int compare() {
        boolean equal = true;
        for (int i = 0; i < dim; i++) {
            if (a[i] != b[i])
                equal = false;
        }
        if (equal)
            return 0;

        resetStateMachine();
        
        // We keep the state of the state machine after a comparison. In most
        // cases,
        // the needed zoom factor is close to the old one. In this step, we
        // check if we have
        // to zoom out
//        while (true) {
//            if (stateStack.size() <= dim) {
//                resetStateMachine();
//                break;
//            }
//            boolean zoomOut = false;
//            for (int i = 0; i < dim; i++) {
//                if (Math.min(a[i], b[i]) <= bounds[i] - 2 * stepsize
//                        || Math.max(a[i], b[i]) >= bounds[i] + 2 * stepsize) {
//                    zoomOut = true;
//                    break;
//                }
//            }
//            state = stateStack.getLast();
//            stateStack.removeLast();
//            for (int j = dim - 1; j >= 0; j--) {
//                bounds[j] = boundsStack.getLast();
//                boundsStack.removeLast();
//            }
//            stepsize *= 2;
//            if (!zoomOut) {
//                state = stateStack.getLast();
//                stateStack.removeLast();
//                for (int j = dim - 1; j >= 0; j--) {
//                    bounds[j] = boundsStack.getLast();
//                    boundsStack.removeLast();
//                }
//                stepsize *= 2;
//                break;
//            }
//        }

        while (true) {
//            stateStack.add(state);
//            for (int j = 0; j < dim; j++) {
//                boundsStack.add(bounds[j]);
//            }

            // Find the quadrant in which A and B are
            int quadrantA = 0, quadrantB = 0;
            for (int i = dim - 1; i >= 0; i--) {
                if (a[i] >= bounds[i])
                    quadrantA ^= (1 << (dim - i - 1));
                if (b[i] >= bounds[i])
                    quadrantB ^= (1 << (dim - i - 1));

                if (a[i] >= bounds[i]) {
                    bounds[i] += stepsize;
                } else {
                    bounds[i] -= stepsize;
                }
            }

            count++;
            
            stepsize /= 2;
            if (stepsize <= 2 * DoublePointable.getEpsilon())
                return 0;
            // avoid infinite loop due to machine epsilon problems
            
            if (quadrantA != quadrantB) {
                // find the position of A and B's quadrants
                int posA = states[state].position[quadrantA];
                int posB = states[state].position[quadrantB];

                if (posA < posB)
                    return -1;
                else
                    return 1;
            }

            state = states[state].nextState[quadrantA];
        }
    }

    @Override
    public abstract int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2);
}
