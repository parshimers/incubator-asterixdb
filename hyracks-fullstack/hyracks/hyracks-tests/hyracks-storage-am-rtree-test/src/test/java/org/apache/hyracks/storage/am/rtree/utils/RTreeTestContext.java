/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hyracks.storage.am.rtree.utils;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.util.SerdeUtils;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.rtree.AbstractRTreeTestContext;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.RTree;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.file.IFileMapProvider;

@SuppressWarnings("rawtypes")
public class RTreeTestContext extends AbstractRTreeTestContext {

    public RTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public int getKeyFieldCount() {
        RTree rtree = (RTree) index;
        return rtree.getComparatorFactories().length;
    }

    @Override
    public IBinaryComparatorFactory[] getComparatorFactories() {
        RTree rtree = (RTree) index;
        return rtree.getComparatorFactories();
    }

    public static RTreeTestContext create(IBufferCache bufferCache, IFileMapProvider fileMapProvider,
            FileReference file, ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeyFields, RTreePolicyType rtreePolicyType)
            throws Exception {
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparatorFactory[] cmpFactories = SerdeUtils.serdesToComparatorFactories(fieldSerdes, numKeyFields);
        RTree rtree = RTreeUtils.createRTree(bufferCache, fileMapProvider, typeTraits, valueProviderFactories,
                cmpFactories, rtreePolicyType, file, true);
        RTreeTestContext testCtx = new RTreeTestContext(fieldSerdes, rtree);
        return testCtx;
    }
}
