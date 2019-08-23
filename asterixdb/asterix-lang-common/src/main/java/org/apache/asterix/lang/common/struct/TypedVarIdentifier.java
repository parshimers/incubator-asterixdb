package org.apache.asterix.lang.common.struct;

import org.apache.asterix.lang.common.expression.TypeExpression;

public class TypedVarIdentifier extends VarIdentifier {
    TypeExpression type;

    public TypedVarIdentifier(int id, String value, TypeExpression type) {
        this.id = id;
        this.type = type;
        this.value = value;
    }

    public TypedVarIdentifier(String value, TypeExpression type) {
        this.id = 0;
        this.type = type;
        this.value = value;
    }

    public void setType(TypeExpression type) {
        this.type = type;
    }

    public TypeExpression getType() {
        return type;
    }

}
