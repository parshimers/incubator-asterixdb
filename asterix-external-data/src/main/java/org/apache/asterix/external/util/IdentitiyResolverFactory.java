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
package org.apache.asterix.external.util;

/**
 * Factory for creating an instance of @see {IdentityResolver}.
 * Identity resolver simply resolves a value to itself and is useful when value being resolved
 * is a node controller id.
 */
public class IdentitiyResolverFactory implements INodeResolverFactory {

    private static INodeResolver INSTANCE = new IdentityResolver();

    @Override
    public INodeResolver createNodeResolver() {
        return INSTANCE;
    }

}
