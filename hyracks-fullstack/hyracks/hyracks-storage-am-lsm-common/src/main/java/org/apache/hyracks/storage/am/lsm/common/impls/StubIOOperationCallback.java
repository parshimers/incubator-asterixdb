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
package org.apache.hyracks.storage.am.lsm.common.impls;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;

public class StubIOOperationCallback implements ILSMIOOperationCallback {

    private List<ILSMComponent> oldComponents = null;
    private ILSMDiskComponent newComponent = null;

    // This class is for testing. It's basically a way to get the new/old component info from the
    // harness callback simply.
    @Override
    public void beforeOperation(LSMOperationType opType) throws HyracksDataException {
        //Not interested in this
    }

    @Override
    public void afterOperation(LSMOperationType opType, List<ILSMComponent> oldComponents,
            ILSMDiskComponent newComponent) throws HyracksDataException {
        this.oldComponents = oldComponents;
        this.newComponent = newComponent;
    }

    @Override
    public synchronized void afterFinalize(LSMOperationType opType, ILSMDiskComponent newComponent)
            throws HyracksDataException {
        //Redundant info from after
    }

    @Override
    public void setNumOfMutableComponents(int count) {
        //Not interested in this
    }

    public List<ILSMComponent> getLastOldComponents() {
        return oldComponents;
    }

    public ILSMDiskComponent getLastNewComponent() {
        return newComponent;
    }
}
