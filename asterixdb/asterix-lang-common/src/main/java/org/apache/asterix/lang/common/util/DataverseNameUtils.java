package org.apache.asterix.lang.common.util;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.visitor.FormatPrintVisitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataverseNameUtils {

   static protected Set<Character> validIdentifierChars = new HashSet<>();
   static protected Set<Character> validIdentifierStartChars = new HashSet<>();

    static {
        for (char ch = 'a'; ch <= 'z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = 'A'; ch <= 'Z'; ++ch) {
            validIdentifierChars.add(ch);
            validIdentifierStartChars.add(ch);
        }
        for (char ch = '0'; ch <= '9'; ++ch) {
            validIdentifierChars.add(ch);
        }
        validIdentifierChars.add('_');
        validIdentifierChars.add('$');
    }

    protected static boolean needQuotes(String str) {
        if (str.length() == 0) {
            return false;
        }
        if (!validIdentifierStartChars.contains(str.charAt(0))) {
            return true;
        }
        for (char ch : str.toCharArray()) {
            if (!validIdentifierChars.contains(ch)) {
                return true;
            }
        }
        return false;
    }

    protected static String normalize(String str) {
        if (needQuotes(str)) {
            return FormatPrintVisitor.revertStringToQuoted(str);
        }
        return str;
    }

    public static String generateDataverseName(DataverseName dataverseName) {
        List<String> dataverseNameParts = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        dataverseNameParts.clear();
        dataverseName.getParts(dataverseNameParts);
        for (int i = 0, ln = dataverseNameParts.size(); i < ln; i++) {
            if (i > 0) {
                sb.append(DataverseName.SEPARATOR_CHAR);
            }
            sb.append(normalize(dataverseNameParts.get(i)));
        }
        return sb.toString();
    }
}
