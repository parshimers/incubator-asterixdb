package org.apache.asterix.external.library.java;

import org.apache.asterix.om.pointables.AListPointable;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IJListAccessor {

    IJObject access(AListPointable pointable, IObjectPool<IJObject, IAType> objectPool, IAType listType,
            JObjectPointableVisitor pointableVisitor) throws HyracksDataException;
}
