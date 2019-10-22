package org.apache.asterix.external.library.java;

import static org.apache.asterix.external.api.ExternalLanguage.*;

import java.net.URL;

import org.apache.asterix.external.api.ExternalLanguage;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.JavaFunctionHelper;
import org.apache.asterix.external.library.PythonFunctionHelper;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.data.std.api.IDataOutputProvider;

public class FunctionHelperFactory {

    public static FunctionHelperFactory INSTANCE = new FunctionHelperFactory();

    private FunctionHelperFactory() {
    }

    public static IFunctionHelper getFunctionHelper(ExternalLanguage lang, IExternalFunctionInfo finfo,
            IAType[] argTypes, IDataOutputProvider outputProvider, URL[] libraryPaths, ClassLoader cl) {
        switch (lang) {
            case JAVA:
                return new JavaFunctionHelper(finfo, argTypes, outputProvider);
            case PYTHON:
                return new PythonFunctionHelper(finfo, argTypes, outputProvider, libraryPaths, cl);
            default:
                return null;
        }
    }
}
