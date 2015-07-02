package org.apache.asterix.external.library;

import org.apache.asterix.external.library.java.IJObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;

public class TypeInfo {

    private IObjectPool<IJObject, IAType> objectPool;
    private IAType atype;
    private ATypeTag typeTag;

    public TypeInfo(IObjectPool<IJObject, IAType> objectPool, IAType atype, ATypeTag typeTag) {
        this.objectPool = objectPool;
        this.atype = atype;
        this.typeTag = typeTag;
    }

    public void reset(IAType atype, ATypeTag typeTag) {
        this.atype = atype;
        this.typeTag = typeTag;
    }

    public IObjectPool<IJObject, IAType> getObjectPool() {
        return objectPool;
    }

    public void setObjectPool(IObjectPool<IJObject, IAType> objectPool) {
        this.objectPool = objectPool;
    }

    public IAType getAtype() {
        return atype;
    }

    public void setAtype(IAType atype) {
        this.atype = atype;
    }

    public ATypeTag getTypeTag() {
        return typeTag;
    }

    public void setTypeTag(ATypeTag typeTag) {
        this.typeTag = typeTag;
    }

}
