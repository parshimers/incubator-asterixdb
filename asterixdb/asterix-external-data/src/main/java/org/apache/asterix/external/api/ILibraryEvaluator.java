package org.apache.asterix.external.api;

import static org.msgpack.core.MessagePack.Code.ARRAY16;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.library.msgpack.MessagePackUtils;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.resources.IDeallocatable;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public interface ILibraryEvaluator extends IDeallocatable {
    static ATypeTag peekArgument(IAType type, IValueReference valueReference) throws HyracksDataException {
        ATypeTag tag = type.getTypeTag();
        if (tag == ATypeTag.ANY) {
            TaggedValuePointable pointy = TaggedValuePointable.FACTORY.createPointable();
            pointy.set(valueReference);
            ATypeTag rtTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(pointy.getTag());
            IAType rtType = TypeTagUtil.getBuiltinTypeByTag(rtTypeTag);
            return MessagePackUtils.peekUnknown(rtType);
        } else {
            return MessagePackUtils.peekUnknown(type);
        }
    }

    static void setVoidArgument(ArrayBackedValueStorage argHolder) throws IOException {
        argHolder.getDataOutput().writeByte(ARRAY16);
        argHolder.getDataOutput().writeShort((short) 0);
    }


    void start() throws IOException, AsterixException;

    long initialize(IExternalFunctionInfo finfo) throws IOException, AsterixException;

    ByteBuffer callPython(long id, IAType[] argTypes, IValueReference[] valueReferences, boolean nullCall)
            throws IOException;

    ByteBuffer callPythonMulti(long id, ArrayBackedValueStorage arguments, int numTuples) throws IOException;
}
