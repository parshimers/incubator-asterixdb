package org.apache.asterix.external.library.java;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.library.TypeInfo;
import org.apache.asterix.external.library.java.JObjectAccessors.JListAccessor;
import org.apache.asterix.external.library.java.JObjectAccessors.JRecordAccessor;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListPointable;
import org.apache.asterix.om.pointables.ARecordPointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class JObjectPointableVisitor implements IVisitablePointableVisitor<IJObject, TypeInfo> {

    private final Map<ATypeTag, IJObjectAccessor> flatJObjectAccessors = new HashMap<ATypeTag, IJObjectAccessor>();
    private final Map<IVisitablePointable, IJRecordAccessor> raccessorToJObject = new HashMap<IVisitablePointable, IJRecordAccessor>();
    private final Map<IVisitablePointable, IJListAccessor> laccessorToPrinter = new HashMap<IVisitablePointable, IJListAccessor>();

    @Override
    public IJObject visit(AListPointable accessor, TypeInfo arg) throws AsterixException {
        IJObject result = null;
        IJListAccessor jListAccessor = laccessorToPrinter.get(accessor);
        if (jListAccessor == null) {
            jListAccessor = new JListAccessor(arg.getObjectPool());
            laccessorToPrinter.put(accessor, jListAccessor);
        }
        try {
            result = jListAccessor.access(accessor, arg.getObjectPool(), arg.getAtype(), this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(ARecordPointable accessor, TypeInfo arg) throws AsterixException {
        IJObject result = null;
        IJRecordAccessor jRecordAccessor = raccessorToJObject.get(accessor);
        if (jRecordAccessor == null) {
            jRecordAccessor = new JRecordAccessor(accessor.getInputRecordType(), arg.getObjectPool());
            raccessorToJObject.put(accessor, jRecordAccessor);
        }
        try {
            result = jRecordAccessor.access(accessor, arg.getObjectPool(), (ARecordType) arg.getAtype(), this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return result;
    }

    @Override
    public IJObject visit(AFlatValuePointable accessor, TypeInfo arg) throws AsterixException {
        ATypeTag typeTag = arg.getTypeTag();
        IJObject result = null;
        IJObjectAccessor jObjectAccessor = flatJObjectAccessors.get(typeTag);
        if (jObjectAccessor == null) {
            jObjectAccessor = JObjectAccessors.createFlatJObjectAccessor(typeTag);
            flatJObjectAccessors.put(typeTag, jObjectAccessor);
        }

        try {
            result = jObjectAccessor.access(accessor, arg.getObjectPool());
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
        return result;
    }

}
