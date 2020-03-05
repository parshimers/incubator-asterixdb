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
package org.apache.asterix.external.library;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExternalScalarFunctionEvaluatorFactory implements IScalarEvaluatorFactory {

    private static final long serialVersionUID = 1L;
    private final IExternalFunctionInfo finfo;
    private final IScalarEvaluatorFactory[] args;
    private final transient IApplicationContext appCtx;
    private final IAType[] argTypes;

    public ExternalScalarFunctionEvaluatorFactory(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args,
            IAType[] argTypes, IApplicationContext appCtx) throws AlgebricksException {
        this.finfo = finfo;
        this.args = args;
        this.argTypes = argTypes;
        this.appCtx = appCtx;
    }

    @Override
    public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
        ExternalScalarFunction fn = (ExternalScalarFunction) ExternalFunctionProvider
                .getExternalFunctionEvaluator(finfo, args, argTypes, ctx, appCtx == null ? (IApplicationContext) ctx
                        .getTaskContext().getJobletContext().getServiceContext().getApplicationContext() : appCtx);
        ctx.getTaskContext().registerDeallocatable(() -> fn.deinitialize());
        return fn;
    }

}
