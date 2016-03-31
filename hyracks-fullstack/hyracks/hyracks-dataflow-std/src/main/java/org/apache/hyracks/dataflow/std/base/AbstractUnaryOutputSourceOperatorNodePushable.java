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
package org.apache.hyracks.dataflow.std.base;

import org.apache.hyracks.api.comm.IFrameWriter;

public abstract class AbstractUnaryOutputSourceOperatorNodePushable extends AbstractUnaryOutputOperatorNodePushable {
    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        throw new IllegalStateException();
    }

    @Override
    public final int getInputArity() {
        return 0;
    }
}
