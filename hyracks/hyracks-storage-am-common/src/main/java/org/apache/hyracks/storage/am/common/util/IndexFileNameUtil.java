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

package org.apache.hyracks.storage.am.common.util;

import java.io.File;

import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;

public class IndexFileNameUtil {

    public static final String IO_DEVICE_NAME_PREFIX = "device_id_";

    @Deprecated
    public static String prepareFileName(String path, int ioDeviceId) {
        return path + File.separator + IO_DEVICE_NAME_PREFIX + ioDeviceId;
    }

    public static FileReference getIndexAbsoluteFileRef(IIndexOperatorDescriptor opDesc, int partition, IIOManager ioManager){
        String indexName = opDesc.getFileSplitProvider().getFileSplits()[partition].getLocalFile().getPath();
        int ioDeviceId = opDesc.getFileSplitProvider().getFileSplits()[partition].getIODeviceId();
        return ioManager.getAbsoluteFileRef(ioDeviceId, indexName);
    }
}