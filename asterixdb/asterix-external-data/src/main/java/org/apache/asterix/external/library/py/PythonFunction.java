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
package org.apache.asterix.external.library.py;

import java.net.URL;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.PythonFunctionHelper;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

public class PythonFunction implements IExternalScalarFunction {

    private static Jep jep;
    private String packageName = "pytestlib";

    @Override
    public void deinitialize() {
        try {
            jep.eval("a.asterixDeinitialize();");
            jep.close();
        } catch (JepException je) {
            //do nothing for now??
        }
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        PythonFunctionHelper pyfh = (PythonFunctionHelper) functionHelper;
        jep.set("asterixArgs", pyfh.getArguments());
        jep.eval("asterixResult = a.nextFrame(asterixArgs)");
        Object result = jep.getValue("asterixResult");
        pyfh.setResult(result);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        packageName = functionHelper.getExternalIdentifier();
        JepConfig jc = new JepConfig();
        for (URL u : ((PythonFunctionHelper) functionHelper).getLibraryDeployedPath()) {
            jc.setIncludePath(u.getPath());
        }
        jc.setClassLoader(((PythonFunctionHelper) functionHelper).getClassLoader());
        jep = jc.createSubInterpreter();
        jep.eval("import " + packageName + " as a");
        jep.eval("a.asterixInitialize()");
    }

}
