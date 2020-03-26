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
package org.apache.asterix.external.library.java;

import java.net.URL;

import org.apache.asterix.common.functions.FunctionLanguage;
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

    public static IFunctionHelper getFunctionHelper(FunctionLanguage lang, IExternalFunctionInfo finfo,
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
