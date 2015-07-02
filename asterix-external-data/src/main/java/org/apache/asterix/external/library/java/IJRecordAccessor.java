package org.apache.asterix.external.library.java;

import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.om.pointables.ARecordPointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IJRecordAccessor {

    public JRecord access(ARecordVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool, ARecordType recordType,
            JObjectPointableVisitor pointableVisitor) throws HyracksDataException;

}
