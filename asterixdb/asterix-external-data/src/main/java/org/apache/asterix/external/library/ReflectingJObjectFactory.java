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

import org.apache.asterix.external.api.IJObject;
import org.apache.asterix.om.util.container.IObjectFactory;

public class ReflectingJObjectFactory implements IObjectFactory<IJObject, Class> {

    public static final ReflectingJObjectFactory INSTANCE = new ReflectingJObjectFactory();

    private ReflectingJObjectFactory() {
    }

    @Override
    public IJObject create(Class clazz) {
        IJObject obj = null;
        try {
            obj = (IJObject) clazz.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return obj;
    }

}
