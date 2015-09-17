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

package org.apache.asterix.api.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class SimpleGpsPointConverter {

    public static void main(String[] args) {

        if (args.length < 3) {
            System.out.println("Usage: java -jar SimpleGpsPointConverter.jar <input file name> <output file name> <sample interval>");
            System.exit(-1);
        }
        String inputFileName = args[0];
        String outputFileName = args[1];
        long sampleInterval = Long.parseLong(args[2]);
        long lineCount = 0;

        BufferedReader br = null;
        FileOutputStream fos = null;
        String line;
        String strPoints[] = null;
        StringBuilder sb = new StringBuilder();
        double points[] = new double[2];
        long id = 0;
        try {
            br = new BufferedReader(new FileReader(inputFileName));
            fos = openOutputFile(outputFileName);
            while ((line = br.readLine()) != null) {
                if (lineCount++ % sampleInterval != 0) {
                    continue;
                }
                sb.setLength(0);
                strPoints = line.split(",");
                if (strPoints.length != 2) {
                    break;
                }
                points[0] = Double.parseDouble(strPoints[0]) / 10000000;  //latitude (y value)
                points[1] = Double.parseDouble(strPoints[1]) / 10000000;  //longitude (x value)
                sb.append("{");
                sb.append("\"id\": ").append(id++);
                //point = x, y (not y, x)
                sb.append(", \"coordinates\": ").append("point(\"").append(points[1]).append(",").append(points[0]).append("\")");
                sb.append("}\n");
                fos.write(sb.toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                closeOutputFile(fos);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static FileOutputStream openOutputFile(String filepath) throws IOException {
        File file = new File(filepath);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        return new FileOutputStream(file);
    }

    public static void closeOutputFile(FileOutputStream fos) throws IOException {
        fos.flush();
        fos.close();
        fos = null;
    }

}