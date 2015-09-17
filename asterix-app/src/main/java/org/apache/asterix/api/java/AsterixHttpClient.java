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


package org.apache.asterix.api.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;

public class AsterixHttpClient {

    private DefaultHttpClient httpclient;
    private HttpGet httpGet;
    private final URIBuilder updateBuilder;
    private final URIBuilder ddlBuilder;
    private final URIBuilder queryBuilder;

    public AsterixHttpClient() throws URISyntaxException {
        httpGet = new HttpGet();
        httpclient = new DefaultHttpClient();
        ddlBuilder = new URIBuilder("http://127.0.0.1:19002/ddl");
        updateBuilder = new URIBuilder("http://127.0.0.1:19002/update");
        queryBuilder = new URIBuilder("http://127.0.0.1:19002/query");
    }

    public AsterixHttpClient(String ccURL, String port) throws URISyntaxException {
        httpGet = new HttpGet();
        httpclient = new DefaultHttpClient();
        ddlBuilder = new URIBuilder("http://" + ccURL + ":" + port + "/ddl");
        updateBuilder = new URIBuilder("http://" + ccURL + ":" + port + "/update");
        queryBuilder = new URIBuilder("http://" + ccURL + ":" + port + "/query");
    }

    public void prepareUpdate(String updateAQL) throws URISyntaxException {
        updateBuilder.setParameter("statements", updateAQL);
        httpGet.setURI(updateBuilder.build());
    }

    public HttpResponse execute() throws ClientProtocolException, IOException {
        return httpclient.execute(httpGet);
    }

    public void prepareDDL(String ddlAQL) throws URISyntaxException {
        ddlBuilder.setParameter("ddl", ddlAQL);
        httpGet.setURI(ddlBuilder.build());
    }

    public void prepareQuery(String queryAQL) throws URISyntaxException {
        queryBuilder.setParameter("query", queryAQL);
        httpGet.setURI(queryBuilder.build());
    }

    public void printResult(HttpResponse response, FileOutputStream fos) throws IllegalStateException, IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        if (fos == null) {
            System.out.println(result.toString());
        } else {
            fos.write(result.toString().getBytes());
            fos.write("\n".getBytes());
        }
    }

    public FileOutputStream openOutputFile(String filepath) throws IOException {
        File file = new File(filepath);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        return new FileOutputStream(file);
    }

    public void closeOutputFile(FileOutputStream fos) throws IOException {
        fos.flush();
        fos.close();
        fos = null;
    }
}
