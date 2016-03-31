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
package org.apache.hyracks.dataflow.common.data.normalizers;

import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputer;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class UTF8StringNormalizedKeyComputerFactory implements INormalizedKeyComputerFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public INormalizedKeyComputer createNormalizedKeyComputer() {
        return new INormalizedKeyComputer() {
            @Override
            public int normalize(byte[] bytes, int start, int length) {
                return UTF8StringUtil.normalize(bytes, start);
            }
        };
    }
}
