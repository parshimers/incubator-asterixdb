package org.apache.asterix.external.library.java.base;

import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;

public abstract class JComplexObject<T> implements IJObject<T> {

    protected IObjectPool<IJObject, IAType> pool;

    public void setPool(IObjectPool<IJObject, IAType> pool) {
        this.pool = pool;
    }
}
