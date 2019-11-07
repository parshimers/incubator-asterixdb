/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.common.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;

/**
 * //TODO(MULTI_PART_DATAVERSE_NAME):JAVADOC
 * assume dataverse parts are ["x", "y", "z"]
 * canonical form is "x.y.z" ('.' and '@' inside part are escaped with '@')
 *
 * display form
 */
public final class DataverseName implements Serializable, Comparable<DataverseName> {

    private static final long serialVersionUID = 1L;

    private static final char SEPARATOR_CHAR = '.';

    private static final char ESCAPE_CHAR = '@';

    private static final char[] SEPARATOR_AND_ESCAPE_CHARS = new char[] { SEPARATOR_CHAR, ESCAPE_CHAR };

    private final boolean isMultiPart;

    private final String canonicalForm;

    private transient volatile String displayForm;

    private DataverseName(String canonicalForm, boolean isMultiPart) {
        if (canonicalForm == null) {
            throw new NullPointerException();
        }
        this.canonicalForm = canonicalForm;
        this.isMultiPart = isMultiPart;
    }

    public boolean isMultiPart() {
        return isMultiPart;
    }

    public String getCanonicalForm() {
        return canonicalForm;
    }

    public List<String> getParts() {
        List<String> parts = new ArrayList<>(isMultiPart ? 4 : 1);
        getParts(parts);
        return parts;
    }

    public void getParts(Collection<? super String> outParts) {
        if (isMultiPart) {
            decodeCanonicalForm(canonicalForm, DataverseName::addPartToCollection, outParts);
        } else {
            outParts.add(decodeSinglePartNameFromCanonicalForm(canonicalForm));
        }
    }

    @Override
    public String toString() {
        return getDisplayForm();
    }

    private String getDisplayForm() {
        String result = displayForm;
        if (result == null) {
            displayForm = result = createDisplayForm();
        }
        return result;
    }

    private String createDisplayForm() {
        if (isMultiPart) {
            StringBuilder displayForm = new StringBuilder(canonicalForm.length() + 1);
            decodeCanonicalForm(canonicalForm, DataverseName::addPartToDisplayForm, displayForm);
            return displayForm.substring(0, displayForm.length() - 1); // remove last separator char
        } else {
            return decodeSinglePartNameFromCanonicalForm(canonicalForm);
        }
    }

    @Override
    public int hashCode() {
        return canonicalForm.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DataverseName)) {
            return false;
        }
        DataverseName that = (DataverseName) obj;
        return canonicalForm.equals(that.canonicalForm);
    }

    @Override
    public int compareTo(DataverseName that) {
        return canonicalForm.compareTo(that.canonicalForm);
    }

    public static DataverseName createFromCanonicalForm(String canonicalForm) {
        boolean isMultiPart = isMultiPartCanonicalForm(canonicalForm);
        return new DataverseName(canonicalForm, isMultiPart);
    }

    public static DataverseName createSinglePartName(String singlePart) {
        String canonicalForm = encodeSinglePartNamePartIntoCanonicalForm(singlePart);
        return new DataverseName(canonicalForm, false);
    }

    public static DataverseName create(List<String> parts) {
        return create(parts, 0, parts.size());
    }

    public static DataverseName create(List<String> parts, int fromIndex, int toIndex) {
        int partCount = toIndex - fromIndex;
        return partCount == 1 ? createSinglePartName(parts.get(fromIndex))
                : createMultiPartName(parts, fromIndex, toIndex);
    }

    private static DataverseName createMultiPartName(List<String> parts, int fromIndex, int toIndex) {
        String canonicalForm = encodeMultiPartNameIntoCanonicalForm(parts, fromIndex, toIndex);
        return new DataverseName(canonicalForm, true);
    }

    private static String encodeMultiPartNameIntoCanonicalForm(List<String> parts, int fromIndex, int toIndex) {
        String firstPart = parts.get(fromIndex);
        int partCount = toIndex - fromIndex;
        if (partCount <= 0) {
            throw new IllegalArgumentException(fromIndex + "," + toIndex);
        }
        int resultSizeEstimate = (firstPart.length() + 1) * partCount;
        StringBuilder sb = new StringBuilder(Math.max(16, resultSizeEstimate));
        encodePartIntoCanonicalForm(firstPart, sb);
        for (int i = fromIndex + 1; i < toIndex; i++) {
            sb.append(SEPARATOR_CHAR);
            encodePartIntoCanonicalForm(parts.get(i), sb);
        }
        return sb.toString();
    }

    private static String encodeSinglePartNamePartIntoCanonicalForm(String singlePart) {
        if (StringUtils.indexOfAny(singlePart, SEPARATOR_AND_ESCAPE_CHARS) < 0) {
            // no escaping needed
            return singlePart;
        }
        StringBuilder sb = new StringBuilder(singlePart.length() + 4);
        encodePartIntoCanonicalForm(singlePart, sb);
        return sb.toString();
    }

    private static void encodePartIntoCanonicalForm(String part, StringBuilder out) {
        for (int i = 0, ln = part.length(); i < ln; i++) {
            char c = part.charAt(i);
            if (c == SEPARATOR_CHAR || c == ESCAPE_CHAR) {
                out.append(ESCAPE_CHAR);
            }
            out.append(c);
        }
    }

    private static <T> void decodeCanonicalForm(String canonicalForm, BiConsumer<CharSequence, T> partConsumer,
            T partConsumerArg) {
        int ln = canonicalForm.length();
        StringBuilder sb = new StringBuilder(ln);
        for (int i = 0; i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case SEPARATOR_CHAR:
                    partConsumer.accept(sb, partConsumerArg);
                    sb.setLength(0);
                    break;
                case ESCAPE_CHAR:
                    i++;
                    c = canonicalForm.charAt(i);
                    // fall through to 'default'
                default:
                    sb.append(c);
                    break;
            }
        }
        if (sb.length() > 0) {
            partConsumer.accept(sb, partConsumerArg);
        }
    }

    // optimization for a single part name
    private String decodeSinglePartNameFromCanonicalForm(String canonicalForm) {
        if (canonicalForm.indexOf(ESCAPE_CHAR) < 0) {
            // no escaping was done
            return canonicalForm;
        }

        StringBuilder singlePart = new StringBuilder(canonicalForm.length());
        for (int i = 0, ln = canonicalForm.length(); i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case SEPARATOR_CHAR:
                    throw new IllegalStateException(canonicalForm); // should never happen
                case ESCAPE_CHAR:
                    i++;
                    c = canonicalForm.charAt(i);
                    // fall through to 'default'
                default:
                    singlePart.append(c);
                    break;
            }
        }
        return singlePart.toString();
    }

    private static boolean isMultiPartCanonicalForm(String canonicalForm) {
        for (int i = 0, ln = canonicalForm.length(); i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case SEPARATOR_CHAR:
                    return true;
                case ESCAPE_CHAR:
                    i++;
                    break;
            }
        }
        return false;
    }

    public static DataverseName createBuiltinDataverseName(String singlePart) {
        if (StringUtils.containsAny(singlePart, SEPARATOR_AND_ESCAPE_CHARS)) {
            throw new IllegalArgumentException(singlePart);
        }
        DataverseName dataverseName = createSinglePartName(singlePart); // 1-part name
        String canonicalForm = dataverseName.getCanonicalForm();
        if (!canonicalForm.equals(singlePart)) {
            throw new IllegalStateException(canonicalForm + "!=" + singlePart);
        }
        return dataverseName;
    }

    private static void addPartToCollection(CharSequence part, Collection<? super String> out) {
        out.add(part.toString());
    }

    private static void addPartToDisplayForm(CharSequence part, StringBuilder out) {
        out.append(part).append(SEPARATOR_CHAR);
    }
}