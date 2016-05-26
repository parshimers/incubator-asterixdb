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

package org.apache.asterix.experiment.action.derived;

import org.apache.asterix.experiment.action.base.AbstractAction;
import org.apache.avro.generic.GenericData;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunSQLPPFileAction extends AbstractAction {
    private static final String REST_URI_TEMPLATE = "http://{0}:{1}/sqlpp";

    private final HttpClient httpClient;

    private final Path aqlFilePath;
    private final List<Path> queriesToRun;

    private final String restHost;

    private final int restPort;

    private final OutputStream os;

    public RunSQLPPFileAction(HttpClient httpClient, String restHost, int restPort, Path aqlFilePath) {
        this.httpClient = httpClient;
        this.aqlFilePath = aqlFilePath;
        this.restHost = restHost;
        this.restPort = restPort;
        os = null;
        queriesToRun = new ArrayList<>();
    }

    public RunSQLPPFileAction(HttpClient httpClient, String restHost, int restPort, Path aqlFilePath, OutputStream os) {
        this.httpClient = httpClient;
        this.aqlFilePath = aqlFilePath;
        this.restHost = restHost;
        this.restPort = restPort;
        this.os = os;
        queriesToRun = new ArrayList<>();
    }

    @Override
    public void doPerform() throws Exception {
        if(aqlFilePath.toFile().isDirectory()){
           for(File f: aqlFilePath.toFile().listFiles()){
               queriesToRun.add(f.toPath());
           }
        }
        else{
            queriesToRun.add(aqlFilePath);
        }
        for(Path p: queriesToRun) {
            String sqlpp = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(Files.readAllBytes(p))).toString();
            String uri = MessageFormat.format(REST_URI_TEMPLATE, restHost, String.valueOf(restPort));
            HttpPost post = new HttpPost(uri);
            post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            post.setEntity(new StringEntity(sqlpp, StandardCharsets.UTF_8));
            long start = System.currentTimeMillis();
            HttpResponse resp = httpClient.execute(post);
            HttpEntity entity = resp.getEntity();
            if(resp.getStatusLine().getStatusCode() != HttpStatus.SC_OK){
                throw new HttpException("Query returned error");
            }
            EntityUtils.consume(entity);
            long end = System.currentTimeMillis();
            long wallClock = end-start;
            System.out.println(p.getFileName().toString() + ',' +wallClock );
        }
    }
}
