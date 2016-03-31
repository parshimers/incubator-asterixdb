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
package org.apache.hyracks.control.nc.application;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.hyracks.api.application.INCApplicationContext;
import org.apache.hyracks.api.application.IStateDumpHandler;
import org.apache.hyracks.api.context.IHyracksRootContext;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.resources.memory.IMemoryManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.control.common.application.ApplicationContext;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.control.nc.resources.memory.MemoryManager;

public class NCApplicationContext extends ApplicationContext implements INCApplicationContext {
    private final ILifeCycleComponentManager lccm;
    private final String nodeId;
    private final IHyracksRootContext rootCtx;
    private final MemoryManager memoryManager;
    private Object appObject;
    private IStateDumpHandler sdh;
    private final NodeControllerService ncs;

    public NCApplicationContext(NodeControllerService ncs, ServerContext serverCtx, IHyracksRootContext rootCtx,
            String nodeId, MemoryManager memoryManager, ILifeCycleComponentManager lifeCyclecomponentManager)
                    throws IOException {
        super(serverCtx);
        this.lccm = lifeCyclecomponentManager;
        this.nodeId = nodeId;
        this.rootCtx = rootCtx;
        this.memoryManager = memoryManager;
        this.ncs = ncs;
        sdh = new IStateDumpHandler() {

            @Override
            public void dumpState(OutputStream os) throws IOException {
                lccm.dumpState(os);
            }
        };
    }

    @Override
    public ILifeCycleComponentManager getLifeCycleComponentManager() {
        return lccm;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    public void setDistributedState(Serializable state) {
        distributedState = state;
    }

    @Override
    public void setStateDumpHandler(IStateDumpHandler handler) {
        this.sdh = handler;
    }

    public IStateDumpHandler getStateDumpHandler() {
        return sdh;
    }

    @Override
    public IHyracksRootContext getRootContext() {
        return rootCtx;
    }

    @Override
    public void setApplicationObject(Object object) {
        this.appObject = object;
    }

    @Override
    public Object getApplicationObject() {
        return appObject;
    }

    @Override
    public IMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public IControllerService getControllerService() {
        return ncs;
    }
}
