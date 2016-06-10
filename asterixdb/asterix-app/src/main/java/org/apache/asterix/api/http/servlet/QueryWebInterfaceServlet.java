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
package org.apache.asterix.api.http.servlet;

import org.apache.commons.io.IOUtils;
import org.stringtemplate.v4.ST;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.imageio.ImageIO;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class QueryWebInterfaceServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private HashMap<String,String> fileMimePair = new HashMap<String,String>();

    public QueryWebInterfaceServlet(){
        fileMimePair.put("png","image/png");
        fileMimePair.put("eot","application/vnd.ms-fontobject");
        fileMimePair.put("svg","image/svg+xml\t");
        fileMimePair.put("ttf","application/x-font-ttf");
        fileMimePair.put("woff","application/x-font-woff");
        fileMimePair.put("woff2","application/x-font-woff");
        fileMimePair.put("html","text/html");
        fileMimePair.put("css","text/css");
        fileMimePair.put("js","application/javascript");
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String resourcePath = null;
        String requestURI = request.getRequestURI();

        if (requestURI.equals("/")) {
            response.setContentType("text/html");
            resourcePath = "/newui/queryui.html";
        } else {
            resourcePath = requestURI;
        }

        try (InputStream is = APIServlet.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            // Multiple MIME type support
            for (Map.Entry<String, String> entry : fileMimePair.entrySet()) {
                if (resourcePath.endsWith(entry.getKey())) {
                    response.setContentType(entry.getValue());
                    OutputStream out = response.getOutputStream();
                    try {
                        IOUtils.copy(is, out);

                    } finally {

                        IOUtils.closeQuietly(out);
                        IOUtils.closeQuietly(is);

                    }
                    return;
                }
            }
            response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    private static boolean isSet(String requestParameter) {
        return (requestParameter != null && requestParameter.equals("true"));
    }
}
