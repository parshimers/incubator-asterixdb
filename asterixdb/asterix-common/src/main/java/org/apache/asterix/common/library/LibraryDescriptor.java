/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.asterix.common.library;

import org.apache.asterix.common.functions.ExternalFunctionLanguage;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IJsonSerializable;
import org.apache.hyracks.api.io.IPersistedResourceRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The information needed to libraries at startup
 */
public class LibraryDescriptor implements IJsonSerializable {

    private static final long serialVersionUID = 1L;
    public static final String DESCRIPTOR_NAME = "descriptor.json";
    /**
     * The library's language
     */
    private final ExternalFunctionLanguage lang;

    public LibraryDescriptor(ExternalFunctionLanguage lang) {
        this.lang = lang;
    }

    public ExternalFunctionLanguage getLang() {
        return lang;
    }

    @Override
    public JsonNode toJson(IPersistedResourceRegistry registry) throws HyracksDataException {
        ObjectNode json = registry.getClassIdentifier(getClass(), serialVersionUID);
        json.put("datasetId", lang.name());
        return json;
    }

    public static IJsonSerializable fromJson(IPersistedResourceRegistry registry, JsonNode json)
            throws HyracksDataException {
        final ExternalFunctionLanguage lang = ExternalFunctionLanguage.valueOf(json.get("lang").asText());
        return new LibraryDescriptor(lang);
    }
}