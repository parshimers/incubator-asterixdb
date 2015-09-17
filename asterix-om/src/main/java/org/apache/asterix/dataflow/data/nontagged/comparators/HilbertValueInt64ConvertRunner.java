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

public class HilbertValueInt64ConvertRunner {

    public static void main(String[] args) {
        HilbertValueInt64Converter hvc = new HilbertValueInt64Converter(2);
        long[][] cell = generateMatrix(hvc, 2);
        printMatrix(cell);
        cell = generateMatrix(hvc, 3);
        printMatrix(cell);
        cell = generateMatrix(hvc, 4);
        printMatrix(cell);
    }

    private static long[][] generateMatrix(HilbertValueInt64Converter hvc, long order) {
        int cellNum = (int) Math.pow(2, order);
        long[][] cell = new long[cellNum][cellNum];
        for (int x = 0; x < cellNum; x++) {
            for (int y = 0; y < cellNum; y++) {
                cell[x][y] = hvc.convert(order, x, y);
            }
        }
        return cell;
    }
    
    private static void printMatrix(long[][] cell) {
        for (int y = 0; y < cell.length; y++) {
            System.out.print("{\t");
            for (int x = 0; x < cell[y].length; x++) {
                if (x == cell[y].length-1)
                    System.out.print(cell[x][y]);
                else
                    System.out.print(cell[x][y] + ",\t");
            }
            if (y == cell.length-1)
                System.out.println("\t}");
            else 
                System.out.println("\t},");
        }
    }
}
