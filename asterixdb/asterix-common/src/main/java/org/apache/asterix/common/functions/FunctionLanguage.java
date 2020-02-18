package org.apache.asterix.common.functions;

import java.util.Locale;

public enum FunctionLanguage {
    // WARNING: do not change these language names because
    // these values are stored in function metadata
    AQL(false),
    SQLPP(false),
    JAVA(true),
    PYTHON(true);

    private final boolean isExternal;

    FunctionLanguage(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public String getName() {
        return name();
    }

    public static FunctionLanguage findByName(String name) {
        return FunctionLanguage.valueOf(name.toUpperCase(Locale.ROOT));
    }
}
