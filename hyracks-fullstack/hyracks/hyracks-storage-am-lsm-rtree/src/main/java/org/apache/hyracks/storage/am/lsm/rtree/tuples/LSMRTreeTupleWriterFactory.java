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

package org.apache.hyracks.storage.am.lsm.rtree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleWriter;

public class LSMRTreeTupleWriterFactory extends TypeAwareTupleWriterFactory {

    private static final long serialVersionUID = 1L;
    private final ITypeTraits[] typeTraits;
    private final boolean isAntimatter;

    public LSMRTreeTupleWriterFactory(ITypeTraits[] typeTraits, boolean isAntimatter) {
        super(typeTraits);
        this.typeTraits = typeTraits;
        this.isAntimatter = isAntimatter;
    }

    @Override
    public ILSMTreeTupleWriter createTupleWriter() {
        return new LSMRTreeTupleWriter(typeTraits, isAntimatter);
    }

}
