package org.apache.asterix.lang.common.struct;

import org.apache.asterix.lang.common.expression.TypeExpression;

public class TypedVarIdentifier extends VarIdentifier {
    TypeExpression type;
    boolean nullable;

    public TypedVarIdentifier(int id, String value, TypeExpression type, boolean nullable) {
        this.id = id;
        this.type = type;
        this.value = value;
        this.nullable = nullable;
    }

    public TypedVarIdentifier(String value, TypeExpression type, boolean nullable) {
        this.id = 0;
        this.type = type;
        this.value = value;
        this.nullable = nullable;
    }

    public void setType(TypeExpression type) {
        this.type = type;
    }

    public TypeExpression getType() {
        return type;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean getNullable() {
        return nullable;
    }

}
