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
package org.apache.asterix.dataflow.data.nontagged;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.IMissingWriter;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AqlMissingWriterFactory implements IMissingWriterFactory {

    private static final long serialVersionUID = 1L;
    public static final AqlMissingWriterFactory INSTANCE = new AqlMissingWriterFactory();

    private AqlMissingWriterFactory() {
    }

    @Override
    public IMissingWriter createMissingWriter() {
        return AqlMissingWriterFactory::writeMissing;
    }

    private static void writeMissing(DataOutput out) throws HyracksDataException {
        try {
            out.writeByte(ATypeTag.SERIALIZED_MISSING_TYPE_TAG);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

}
