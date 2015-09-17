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

package org.apache.asterix.dataflow.data.common;

import java.io.UnsupportedEncodingException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IBinaryTokenizer;
import org.apache.hyracks.storage.am.common.api.IToken;
import org.apache.hyracks.storage.am.common.api.ITokenFactory;

public class SIFBinaryTokenizer implements IBinaryTokenizer {

    private final double bottomLeftX;
    private final double bottomLeftY;
    private final double topRightX;
    private final double topRightY;
    private final long xCellNum;
    private final long yCellNum;
    private final double xCellSize;
    private final double yCellSize;
    private final int frameSize;
    private final IToken token;
    private final long[] cellId;
    private final StringBuilder sb;
    private byte[] inputData;
    private boolean isNull;
    private long maxX;
    private long maxY;
    private long minX;
    private long minY;
    private long curX;
    private long curY;
    private boolean oops = false;
    private static final String OOPS = "OOPS";
    private static byte[] OOPS_BYTE_ARRAY;

    public SIFBinaryTokenizer(double bottomLeftX, double bottomLeftY, double topRightX, double topRightY,
            short[] levelDensity, int cellsPerObject, ITokenFactory tokenFactory, int frameSize) {
        this.frameSize = frameSize;
        this.bottomLeftX = bottomLeftX;
        this.bottomLeftY = bottomLeftY;
        this.topRightX = topRightX;
        this.topRightY = topRightY;
        long cellNum = 1;
        for (int i = 0; i < levelDensity.length; i++) {
            cellNum *= levelDensity[i];
        }
        this.xCellNum = cellNum;
        this.yCellNum = cellNum;

        this.xCellSize = (topRightX - bottomLeftX) / xCellNum;
        this.yCellSize = (topRightX - bottomLeftX) / yCellNum;
        this.cellId = new long[2];
        token = tokenFactory.createToken();
        sb = new StringBuilder();
        try {
            OOPS_BYTE_ARRAY = OOPS.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e); //this should never happen for UTF-8 encoding.
        }
    }

    @Override
    public IToken getToken() {
        return token;
    }

    @Override
    public boolean hasNext() {
        return oops || curX <= maxX || curY <= maxY;
    }

    @Override
    public void next() throws HyracksDataException {
        if (oops) {
            token.reset(OOPS_BYTE_ARRAY, 0, OOPS_BYTE_ARRAY.length, OOPS.length(), 1);
            oops = false;
            return;
        }

        sb.setLength(0);
        sb.append(curX).append("C").append(curY);
        String strCellId = sb.toString();
        byte[] bytearr;
        try {
            bytearr = strCellId.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HyracksDataException(e);
        }
        token.reset(bytearr, 0, bytearr.length, strCellId.length(), 1);

        //adjust curX and curY
        ++curX;
        if (curX > maxX) {
            curY++;
            if (curY <= maxY) {
                curX = minX;
            }
        }
    }

    @Override
    public void reset(byte[] data, int start, int length) throws HyracksDataException {
        this.inputData = data;
        oops = false;

        //consider null value
        if (length <= 1) {
            curX = 1;
            maxX = 0;
            curY = 1;
            maxY = 0;
        }

        //check type tag
        if (inputData[start] == ATypeTag.POINT.serialize()) {
            double x = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + APointSerializerDeserializer.getCoordinateOffset(Coordinate.X));
            double y = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + APointSerializerDeserializer.getCoordinateOffset(Coordinate.Y));
            oops = isOOPS(x, y);
            if (oops) {
                curX = 1;
                maxX = 0;
                curY = 1;
                maxY = 0;
            } else {
                computeCellId(x, y, cellId);
                curX = maxX = minX = cellId[0];
                curY = maxY = minY = cellId[1];
            }
        } else if (inputData[start] == ATypeTag.RECTANGLE.serialize()) {
            double x1 = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.X));
            double y1 = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + ARectangleSerializerDeserializer.getBottomLeftCoordinateOffset(Coordinate.Y));
            double x2 = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.X));
            double y2 = ADoubleSerializerDeserializer.getDouble(inputData,
                    start + ARectangleSerializerDeserializer.getUpperRightCoordinateOffset(Coordinate.Y));

            //handle OOPS cases
            //case1: QueryBottomeLeft is OOPS & QueryTopRight is OOPS  
            //  -> set oops = true and set curX, maxX, curY, maxX in such a way that hasNext() return false, done!
            //case2: QueryBottomeLeft is OOPS & QueryTopRight is not   
            //  -> set oops = true and QueryBottomLeft = SpaceBottomLeft, done!
            //case3: QueryBottomeLeft is not & QueryTopRight is OOPS   
            //  -> set oops = true and QueryTopRight = SpaceTopRight, done!
            //case4: QueryBottomeLeft is not & QueryTopRight is not   
            //  -> set oops = false, done!
            boolean bOOPS = isOOPS(x1, y1);
            boolean tOOPS = isOOPS(x2, y2);
            if (bOOPS && tOOPS) { //case1
                oops = true;
                curX = 1;
                maxX = 0;
                curY = 1;
                maxY = 0;
            } else if (bOOPS && !tOOPS) { //case2
                oops = true;
                x1 = bottomLeftX;
                y1 = bottomLeftY;
            } else if (!bOOPS && tOOPS) { //case3
                oops = true;
                x2 = topRightX;
                y2 = topRightY;
            } else { //case4
                oops = false;
            }

            computeCellId(x1, y1, cellId);
            curX = minX = cellId[0];
            curY = minY = cellId[1];
            computeCellId(x2, y2, cellId);
            maxX = cellId[0];
            maxY = cellId[1];
        } else {
            throw new HyracksDataException("SIFBinaryTokenizer: unsupported type tag: " + inputData[start]);
        }
    }

    private void computeCellId(double x, double y, long[] cellId) {
        cellId[0] = (long) Math.floor((x - bottomLeftX) / xCellSize);
        cellId[1] = (long) Math.floor((y - bottomLeftY) / yCellSize);
    }

    private boolean isOOPS(double x, double y) { //out of point space
        return !(x >= bottomLeftX && x <= topRightX && y >= bottomLeftY && y <= topRightY);
    }

    @Override
    public short getTokensCount() {
        return 0;
    }

}
