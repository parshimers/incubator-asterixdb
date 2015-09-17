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

package org.apache.asterix.runtime.evaluators.functions;

import org.apache.asterix.common.config.DatasetConfig.IndexTypeProperty;
import org.apache.asterix.dataflow.data.common.SIFBinaryTokenizer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.WordTokensEvaluator;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.storage.am.common.api.IBinaryTokenizer;
import org.apache.hyracks.storage.am.common.api.ITokenFactory;
import org.apache.hyracks.storage.am.common.tokenizer.UTF8WordTokenFactory;

public class SIFTokensDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SIFTokensDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SIF_TOKENS;
    }

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                ArrayBackedValueStorage outBottomLeftX = new ArrayBackedValueStorage();
                ArrayBackedValueStorage outBottomLeftY = new ArrayBackedValueStorage();
                ArrayBackedValueStorage outTopRightX = new ArrayBackedValueStorage();
                ArrayBackedValueStorage outTopRightY = new ArrayBackedValueStorage();

                int maxLevel = IndexTypeProperty.CELL_BASED_SPATIAL_INDEX_MAX_LEVEL;
                short[] levelDensity = new short[maxLevel];
                ArrayBackedValueStorage[] outLevelDensity = new ArrayBackedValueStorage[maxLevel];
                ArrayBackedValueStorage outCellsPerObject = new ArrayBackedValueStorage();
                ArrayBackedValueStorage outFrameSize = new ArrayBackedValueStorage();

                args[1].createEvaluator(outBottomLeftX).evaluate(null);
                args[2].createEvaluator(outBottomLeftY).evaluate(null);
                args[3].createEvaluator(outTopRightX).evaluate(null);
                args[4].createEvaluator(outTopRightY).evaluate(null);
                for (int i = 0; i < maxLevel; i++) {
                    outLevelDensity[i] = new ArrayBackedValueStorage();
                    args[5 + i].createEvaluator(outLevelDensity[i]).evaluate(null);
                }
                args[5 + maxLevel].createEvaluator(outCellsPerObject).equals(null);
                args[5 + maxLevel + 1].createEvaluator(outFrameSize).equals(null);

                double bottomLeftX = ADoubleSerializerDeserializer.getDouble(outBottomLeftX.getByteArray(), 1);
                double bottomLeftY = ADoubleSerializerDeserializer.getDouble(outBottomLeftY.getByteArray(), 1);
                double topRightX = ADoubleSerializerDeserializer.getDouble(outTopRightX.getByteArray(), 1);
                double topRightY = ADoubleSerializerDeserializer.getDouble(outTopRightY.getByteArray(), 1);
                for (int i = 0; i < maxLevel; i++) {
                    levelDensity[i] = AInt16SerializerDeserializer.getShort(outLevelDensity[i].getByteArray(), 1);
                }
                int cellsPerObject = AInt32SerializerDeserializer.getInt(outCellsPerObject.getByteArray(), 1);
                int frameSize = AInt32SerializerDeserializer.getInt(outFrameSize.getByteArray(), 1);

                ITokenFactory tokenFactory = new UTF8WordTokenFactory();
                IBinaryTokenizer tokenizer = new SIFBinaryTokenizer(bottomLeftX, bottomLeftY, topRightX, topRightY,
                        levelDensity, cellsPerObject, tokenFactory, frameSize);
                return new WordTokensEvaluator(args, output, tokenizer, BuiltinType.ASTRING);
            }
        };
    }

}
