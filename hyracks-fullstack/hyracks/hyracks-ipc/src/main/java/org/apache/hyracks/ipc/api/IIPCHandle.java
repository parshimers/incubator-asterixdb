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
package org.apache.hyracks.ipc.api;

import java.net.InetSocketAddress;

import org.apache.hyracks.ipc.exceptions.IPCException;

public interface IIPCHandle {
    public InetSocketAddress getRemoteAddress();

    public long send(long requestId, Object payload, Exception exception) throws IPCException;

    public void setAttachment(Object attachment);

    public Object getAttachment();

    public boolean isConnected();
}
