package org.apache.asterix.external.library.java;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;

public interface IJType {

    public ATypeTag getTypeTag();

    public IAObject getIAObject();
}
