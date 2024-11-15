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
package edu.uci.ics.asterix.lexergenerator.rules;

public class RulePartial implements Rule {

    private String partialName;

    public RulePartial clone() {
        return new RulePartial(partialName);
    }

    public RulePartial(String expected) {
        this.partialName = expected;
    }

    public String getPartial() {
        return this.partialName;
    }

    @Override
    public String toString() {
        return partialName;
    }

    @Override
    public int hashCode() {
        return (int) partialName.charAt(1);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof RulePartial) {
            if (((RulePartial) o).partialName.equals(this.partialName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "";
    }

    @Override
    public String javaMatch(String action) {
        StringBuilder result = new StringBuilder();
        result.append("if (parse_" + partialName + "(currentChar)==TOKEN_" + partialName + "){");
        result.append("currentChar = buffer[bufpos];");
        result.append(action);
        result.append("}");
        result.append("else { currentChar = buffer[bufpos];}");
        return result.toString();
    }

}
