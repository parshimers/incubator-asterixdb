package org.apache.asterix.translator;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.runtime.base.IUnnestingPositionWriter;

public class AqlPositionWriter implements IUnnestingPositionWriter, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void write(DataOutput dataOutput, long position) throws IOException {
        dataOutput.writeByte(BuiltinType.AINT64.getTypeTag().serialize());
        dataOutput.writeLong(position);
    }

}
