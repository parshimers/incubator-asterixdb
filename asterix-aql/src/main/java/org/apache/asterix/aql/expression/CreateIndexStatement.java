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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.aql.base.Expression;
import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.config.DatasetConfig.IndexType;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CreateIndexStatement implements Statement {

    private Identifier indexName;
    private Identifier dataverseName;
    private Identifier datasetName;
    private List<Pair<List<String>, TypeExpression>> fieldExprs = new ArrayList<Pair<List<String>, TypeExpression>>();
    private IndexType indexType = IndexType.BTREE;
    private boolean enforced;
    private boolean ifNotExists;

    // Specific to NGram indexes.
    private int gramLength;

    public CreateIndexStatement() {
    }

    public void setGramLength(int gramLength) {
        this.gramLength = gramLength;
    }

    public int getGramLength() {
        return gramLength;
    }

    public Identifier getIndexName() {
        return indexName;
    }

    public void setIndexName(Identifier indexName) {
        this.indexName = indexName;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(Identifier dataverseName) {
        this.dataverseName = dataverseName;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(Identifier datasetName) {
        this.datasetName = datasetName;
    }

    public List<Pair<List<String>, TypeExpression>> getFieldExprs() {
        return fieldExprs;
    }

    public void addFieldExprPair(Pair<List<String>, TypeExpression> fp) {
        this.fieldExprs.add(fp);
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public boolean isEnforced() {
        return enforced;
    }

    public void setEnforced(boolean isEnforced) {
        this.enforced = isEnforced;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_INDEX;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreateIndexStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
