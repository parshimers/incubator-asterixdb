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

package org.apache.asterix.common.metadata;

import java.io.Serializable;
import java.util.Objects;

public final class MetadataLockKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private final MetadataEntityKind entityKind;

    private final String entityKindExtension;

    private final DataverseName dataverseName;

    private final String entityName;

    public MetadataLockKey(MetadataEntityKind entityKind, String entityKindExtension, DataverseName dataverseName,
            String entityName) {
        if (entityKind == null || (dataverseName == null && entityName == null)) {
            throw new NullPointerException();
        }
        this.entityKind = entityKind;
        this.entityKindExtension = entityKindExtension;
        this.dataverseName = dataverseName;
        this.entityName = entityName;
    }

    public MetadataEntityKind getEntityKind() {
        return entityKind;
    }

    public String getEntityKindExtension() {
        return entityKindExtension;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getEntityName() {
        return entityName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MetadataLockKey that = (MetadataLockKey) o;
        return entityKind == that.entityKind && Objects.equals(entityKindExtension, that.entityKindExtension)
                && Objects.equals(dataverseName, that.dataverseName) && Objects.equals(entityName, that.entityName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityKind, entityKindExtension, dataverseName, entityName);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(64);
        sb.append(entityKind);
        if (entityKindExtension != null) {
            sb.append(':').append(entityKindExtension);
        }
        if (dataverseName != null) {
            sb.append(':').append(dataverseName.getCanonicalForm());
        }
        sb.append(':').append(entityName);
        return sb.toString();
    }
}