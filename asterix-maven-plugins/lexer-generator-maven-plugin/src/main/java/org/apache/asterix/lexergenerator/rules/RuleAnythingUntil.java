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

public class RuleAnythingUntil implements Rule {

    private char expected;

    public RuleAnythingUntil clone() {
        return new RuleAnythingUntil(expected);
    }

    public RuleAnythingUntil(char expected) {
        this.expected = expected;
    }

    @Override
    public String toString() {
        return " .* " + String.valueOf(expected);
    }

    @Override
    public int hashCode() {
        return 10 * (int) expected;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof RuleAnythingUntil) {
            if (((RuleAnythingUntil) o).expected == this.expected) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "currentChar = readNextChar();";
    }

    @Override
    public String javaMatch(String action) {
        return "boolean escaped = false;\n" + "while (currentChar != '" + expected + "' || escaped) {\n"
                + "if(!escaped && currentChar == '\\\\\\\\') {\n" + "escaped = true;\n" + "containsEscapes = true;\n"
                + "} else {\n" + "escaped = false;\n" + "}\n" + "currentChar = readNextChar();\n" + "}\n"
                + "if (currentChar == '" + expected + "') {" + action + "}\n";
    }

}
