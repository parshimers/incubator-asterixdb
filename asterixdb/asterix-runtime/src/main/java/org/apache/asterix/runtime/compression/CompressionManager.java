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
package org.apache.asterix.runtime.compression;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.storage.ICompressionManager;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.compression.ICompressorDecompressorFactory;
import org.apache.hyracks.api.compression.impl.LZ4CompressorDecompressorFactory;
import org.apache.hyracks.api.compression.impl.LZ4HCCompressorDecompressorFactory;
import org.apache.hyracks.api.compression.impl.SnappyCompressorDecompressorFactory;
import org.apache.hyracks.api.io.IJsonSerializable;

public class CompressionManager implements ICompressionManager {
    //Compression Schemes
    private static final Map<String, Class<? extends ICompressorDecompressorFactory>> REGISTERED_SCHEMES =
            new HashMap<>();
    private static final String NONE = "none";
    //Default configurations
    private final String defaultScheme;

    public CompressionManager(StorageProperties storageProperties) throws AlgebricksException {
        registerSchemes();
        validateCompressionConfiguration(storageProperties);
        defaultScheme = storageProperties.getCompressionScheme();
    }

    /*
     * New compression schemes can be added by registering the name and the factory class
     *
     * WARNING: Changing scheme name will breakdown storage back compatibility. Before upgrading to a newer
     * version of the registered schemes, make sure it is also back-compatible with the previous version.
     */
    private static void registerSchemes() {
        if (REGISTERED_SCHEMES.size() > 0) {
            return;
        }
        //No compression
        REGISTERED_SCHEMES.put(NONE, null);
        //Version 1.7.1.1
        REGISTERED_SCHEMES.put("snappy", SnappyCompressorDecompressorFactory.class);
        //Version 1.4.1
        REGISTERED_SCHEMES.put("lz4", LZ4CompressorDecompressorFactory.class);
        //Version 1.4.1
        REGISTERED_SCHEMES.put("lz4hc", LZ4HCCompressorDecompressorFactory.class);
    }

    @Override
    public ICompressorDecompressorFactory getFactory(String schemeName, boolean isPrimary) throws CompilationException {

        if (!isPrimary) {
            //Compression is only supported for primary index.
            return null;
        }

        return getFactory(getCompressionScheme(schemeName));
    }

    /**
     * Register factory classes for persisted resources
     *
     * @param registeredClasses
     */
    public static void registerBlockCompressorDecompressorsFactoryClasses(
            Map<String, Class<? extends IJsonSerializable>> registeredClasses) {
        registerSchemes();
        for (Class<? extends ICompressorDecompressorFactory> clazz : REGISTERED_SCHEMES.values()) {
            if (clazz != null) {
                registeredClasses.put(clazz.getSimpleName(), clazz);
            }
        }
    }

    /**
     * @param schemeName
     *            Compression scheme name
     * @return
     *         true if it is registered
     */
    private boolean isRegisteredScheme(String schemeName) {
        return schemeName != null && REGISTERED_SCHEMES.containsKey(schemeName.toLowerCase());
    }

    /**
     * To validate the configuration of StorageProperties
     *
     * @param storageProperties
     */
    private void validateCompressionConfiguration(StorageProperties storageProperties) throws AlgebricksException {
        if (!isRegisteredScheme(storageProperties.getCompressionScheme())) {
            final String option = StorageProperties.Option.STORAGE_COMPRESSION_SCHEME.ini();
            final String value = storageProperties.getCompressionScheme();
            throw new CompilationException(ErrorCode.INVALID_COMPRESSION_CONFIG, option, value,
                    formatSupportedValues(REGISTERED_SCHEMES.keySet()));
        }

    }

    private ICompressorDecompressorFactory getFactory(String scheme) throws CompilationException {
        if (NONE.equals(scheme)) {
            return null;
        }

        Class<? extends ICompressorDecompressorFactory> clazz = REGISTERED_SCHEMES.get(scheme);
        try {
            return clazz.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new CompilationException(ErrorCode.FAILED_TO_INIT_COMPRESSOR_FACTORY, e, scheme);
        }
    }

    private String getCompressionScheme(String ddlScheme) throws CompilationException {
        if (ddlScheme != null && !isRegisteredScheme(ddlScheme)) {
            throw new CompilationException(ErrorCode.UNKNOWN_COMPRESSION_SCHEME, ddlScheme,
                    formatSupportedValues(REGISTERED_SCHEMES.keySet()));
        }

        return ddlScheme != null ? ddlScheme : defaultScheme;
    }

    private String formatSupportedValues(Collection<String> supprtedValues) {
        final StringBuilder schemes = new StringBuilder();
        final Iterator<String> iterator = supprtedValues.iterator();
        schemes.append('[');
        schemes.append(iterator.next());
        while (iterator.hasNext()) {
            schemes.append(',');
            schemes.append(iterator.next());
        }
        schemes.append(']');
        return schemes.toString();
    }
}
