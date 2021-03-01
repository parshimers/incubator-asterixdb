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
package org.apache.asterix.app.external;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

@SuppressWarnings("squid:S134")
public class ExternalUDFLibrarian implements IExternalUDFLibrarian {

    private HttpClient hc;

    public ExternalUDFLibrarian() {
        hc = new DefaultHttpClient();
    }

    @Override
    public void install(URI path, String dataverseKey, DataverseName dataverse, String name, String type,
            String libPath, Pair<String, String> credentials) throws Exception {
        HttpClientContext hcCtx = createHttpClientContext(path, credentials);
        HttpPost post = new HttpPost(path);
        File lib = new File(libPath);
        MultipartEntityBuilder entity = MultipartEntityBuilder.create().setMode(HttpMultipartMode.STRICT);
        entity.addTextBody(dataverseKey, dataverse.toString());
        entity.addTextBody("name", name);
        entity.addTextBody("type", type);
        entity.addBinaryBody("data", lib, ContentType.DEFAULT_BINARY, lib.getName()).build();
        post.setEntity(entity.build());
        HttpResponse response = hc.execute(post, hcCtx);
        handleResponse(response);
    }

    @Override
    public void uninstall(URI path, DataverseName dataverse, String name, Pair<String, String> credentials)
            throws IOException, AsterixException {
        HttpClientContext hcCtx = createHttpClientContext(path, credentials);
        HttpPost post = new HttpPost(path);
        MultipartEntityBuilder entity = MultipartEntityBuilder.create().setMode(HttpMultipartMode.STRICT);
        entity.addTextBody("dataverse", dataverse.toString());
        entity.addTextBody("name", name);
        entity.addTextBody("delete", "true");
        post.setEntity(entity.build());
        HttpResponse response = hc.execute(post, hcCtx);
        handleResponse(response);
    }

    @Override
    public void test(URI path, Pair<String, String> credentials, List<Triple<String, String, String>> fields)
            throws IOException, AsterixException {
        HttpClientContext hcCtx = createHttpClientContext(path, credentials);
        HttpPost post = new HttpPost(path);
        MultipartEntityBuilder entity = MultipartEntityBuilder.create().setMode(HttpMultipartMode.STRICT);
        for (Triple<String, String, String> field : fields) {
            if ("string".equals(field.third)) {
                entity.addTextBody(field.first, field.second);
            } else {
                File binaryData = new File(field.second);
                entity.addBinaryBody(field.first, binaryData);
            }
        }
        post.setEntity(entity.build());
        HttpResponse response = hc.execute(post, hcCtx);
        handleResponse(response);
    }

    private HttpClientContext createHttpClientContext(URI path, Pair<String, String> credentials) {
        HttpClientContext hcCtx = HttpClientContext.create();
        HttpHost h = new HttpHost(path.getHost(), path.getPort(), "http");
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(credentials.first, credentials.second));
        hcCtx.setCredentialsProvider(cp);
        AuthCache ac = new BasicAuthCache();
        ac.put(h, new BasicScheme());
        hcCtx.setAuthCache(ac);
        return hcCtx;
    }

    private void handleResponse(HttpResponse response) throws IOException, AsterixException {
        String resp = null;
        int respCode = response.getStatusLine().getStatusCode();
        if (respCode == 500 || respCode == 400) {
            resp = IOUtils.toString(response.getEntity().getContent());
        }
        response.getEntity().consumeContent();
        if (resp == null && respCode != 200) {
            resp = response.getStatusLine().toString();
        }
        if (resp != null) {
            throw new AsterixException(resp);
        }
    }
}
