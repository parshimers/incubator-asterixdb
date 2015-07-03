package edu.uci.ics.asterix.external.library.java;

import edu.uci.ics.asterix.om.pointables.AListVisitablePointable;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IJListAccessor {

    IJObject access(AListVisitablePointable pointable, IObjectPool<IJObject, IAType> objectPool, IAType listType,
            JObjectPointableVisitor pointableVisitor) throws HyracksDataException;
}
