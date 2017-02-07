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

package org.apache.hyracks.storage.am.lsm.rtree.impls;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentFilterFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponentFactory;
import org.apache.hyracks.storage.am.lsm.common.impls.LSMComponentFileReferences;
import org.apache.hyracks.storage.am.lsm.common.impls.TreeIndexFactory;
import org.apache.hyracks.storage.am.rtree.impls.RTree;

public class LSMRTreeWithAntiMatterTuplesDiskComponentFactory implements ILSMDiskComponentFactory {
    private final TreeIndexFactory<RTree> rtreeFactory;
    private final ILSMComponentFilterFactory filterFactory;

    public LSMRTreeWithAntiMatterTuplesDiskComponentFactory(TreeIndexFactory<RTree> rtreeFactory,
            ILSMComponentFilterFactory filterFactory) {
        this.rtreeFactory = rtreeFactory;
        this.filterFactory = filterFactory;
    }

    @Override
    public LSMRTreeDiskComponent createComponent(LSMComponentFileReferences cfr) throws HyracksDataException {
        return new LSMRTreeDiskComponent(rtreeFactory.createIndexInstance(cfr.getInsertIndexFileReference()), null,
                null, filterFactory == null ? null : filterFactory.createFilter());
    }
}
