/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.external.ipc;

public enum MessageType {
    HELO,
    QUIT,
    INIT,
    INIT_RSP,
    CALL,
    CALL_RSP,
    ERROR,
    INIT_CLASS,
    INIT_CLASS_RSP;

    static MessageType[] messageTypes;
    static {
        messageTypes = values();
    }

    public static MessageType fromByte(byte b) {
        if (b > messageTypes.length - 1) {
            return null;
        }
        return messageTypes[b];
    }
}
