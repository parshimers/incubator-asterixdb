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

public class FsqCheckinTweetOpenStreetMapPointSynthesis {

    public static void main(String[] args) {

        if (args.length < 4) {
            System.out.println("Usage: java -jar FsqCheckinTweetOpenStreetMapPointSynthesis.jar <OpenStreeMapPoint file name> <input adm format file name> <output file name> <sample interval for OpenStreetMapPoint>");
            System.exit(-1);
        }
        String inputFileName1 = args[0];
        String inputFileName2 = args[1];
        String outputFileName = args[2];
        long sampleIntervalForInputFile2 = Long.parseLong(args[3]);
        
        long lineCount = 0;

        BufferedReader file1 = null;
        BufferedReader file2 = null;
        FileOutputStream fos = null;
        String line1;
        String line2;
        String strPoints[] = null;
        StringBuilder sb = new StringBuilder();
        double points[] = new double[2];
        try {
            file1 = new BufferedReader(new FileReader(inputFileName1));
            file2 = new BufferedReader(new FileReader(inputFileName2));
            fos = openOutputFile(outputFileName);
            //read a line from file1
            while ((line1 = file1.readLine()) != null) {
                if (lineCount++ % sampleIntervalForInputFile2 != 0) {
                    continue;
                }
                
                //read a line from file2
                if ((line2 = file2.readLine()) == null) {
                    break;
                }
                
                //parse line1 and convert it into a point
                sb.setLength(0);
                strPoints = line1.split(",");
                if (strPoints.length != 2) {
                    break;
                }
                points[0] = Double.parseDouble(strPoints[0]) / 10000000; //latitude or y
                points[1] = Double.parseDouble(strPoints[1]) / 10000000; //longitude or x
                
                //replace the point in line2 from file2 with the point parsed from line1
                sb.append(line2.substring(0, line2.indexOf("point(\""))).append("point(\"").append(points[1]).append(",").append(points[0]).append("\")");
                if (line2.indexOf("\"url\"") != -1) {
                    sb.append(", ").append(line2.substring(line2.indexOf("\"url\""))).append("\n");
                } else {
                    sb.append(" }\n");
                }
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