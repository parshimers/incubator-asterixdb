package org.apache.asterix.external.library.java;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IJObjectAccessor {
    IJObject access(IVisitablePointable pointable, IObjectPool<IJObject, IAType> obj) throws HyracksDataException;

}
