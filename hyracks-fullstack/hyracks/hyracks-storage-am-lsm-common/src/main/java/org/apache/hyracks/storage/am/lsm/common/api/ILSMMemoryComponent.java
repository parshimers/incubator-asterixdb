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
package org.apache.hyracks.storage.am.lsm.common.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.impls.MemoryComponentMetadata;

public interface ILSMMemoryComponent extends ILSMComponent {
    @Override
    default LSMComponentType getType() {
        return LSMComponentType.MEMORY;
    }

    @Override
    MemoryComponentMetadata getMetadata();

    /**
     * @return true if the component can be entered for reading
     */
    boolean isReadable();

    /**
     * @return the number of writers inside the component
     */
    int getWriterCount();

    /**
     * Clear the component and its metadata page completely
     *
     * @throws HyracksDataException
     */
    void reset() throws HyracksDataException;

    /**
     * @return true if the memory budget has been exceeded
     */
    boolean isFull();

    /**
     * @return true if there are data in the memory component, false otherwise
     */
    boolean isModified();

    /**
     * Set the component as modified
     */
    void setModified();

    /**
     * request the component to be active
     */
    void requestActivation();

    /**
     * Set the component state
     *
     * @param state
     *            the new state
     */
    void setState(ComponentState state);

    /**
     * Allocates memory to this component, create and activate it
     *
     * @throws HyracksDataException
     */
    void allocate() throws HyracksDataException;

    /**
     * Deactivete the memory component, destroy it, and deallocates its memory
     *
     * @throws HyracksDataException
     */
    void deallocate() throws HyracksDataException;

    /**
     * Test method
     * TODO: Get rid of it
     *
     * @throws HyracksDataException
     */
    void validate() throws HyracksDataException;

    /**
     * @return the size of the memory component
     */
    long getSize();

    /**
     * Reset the component Id of the memory component after it's recycled
     *
     * @param newId
     * @throws HyracksDataException
     */
    void resetId(ILSMComponentId newId) throws HyracksDataException;
}
