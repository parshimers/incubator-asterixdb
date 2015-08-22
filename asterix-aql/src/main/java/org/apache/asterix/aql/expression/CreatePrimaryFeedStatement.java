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

import java.util.Map;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.hyracks.algebricks.common.utils.Pair;

public class CreatePrimaryFeedStatement extends CreateFeedStatement implements Statement {

    private final String adaptorName;
    private final Map<String, String> adaptorConfiguration;

    public CreatePrimaryFeedStatement(Pair<Identifier, Identifier> qName, String adaptorName,
            Map<String, String> adaptorConfiguration, FunctionSignature appliedFunction, boolean ifNotExists) {
        super(qName, appliedFunction, ifNotExists);
        this.adaptorName = adaptorName;
        this.adaptorConfiguration = adaptorConfiguration;
    }

    public String getAdaptorName() {
        return adaptorName;
    }

    public Map<String, String> getAdaptorConfiguration() {
        return adaptorConfiguration;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_PRIMARY_FEED;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreatePrimaryFeedStatement(this, arg);
    }
}
