/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.aql.expression;

import java.util.List;

import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class UpdateStatement implements Statement {

    private VariableExpr vars;
    private Expression target;
    private Expression condition;
    private List<UpdateClause> ucs;

    public UpdateStatement(VariableExpr vars, Expression target, Expression condition, List<UpdateClause> ucs) {
        this.vars = vars;
        this.target = target;
        this.condition = condition;
        this.ucs = ucs;
    }

    @Override
    public Kind getKind() {
        return Kind.UPDATE;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Expression getTarget() {
        return target;
    }

    public Expression getCondition() {
        return condition;
    }

    public List<UpdateClause> getUpdateClauses() {
        return ucs;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitUpdateStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
