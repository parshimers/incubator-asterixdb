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
package org.apache.hyracks.http.server;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.http.api.IServlet;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

public class HttpRequestHandler implements Callable<Void> {
    private static final Logger LOGGER = Logger.getLogger(HttpRequestHandler.class.getName());
    private final ChannelHandlerContext ctx;
    private final IServlet servlet;
    private final IServletRequest request;
    private final IServletResponse response;

    public HttpRequestHandler(ChannelHandlerContext ctx, IServlet servlet, IServletRequest request, int chunkSize) {
        this.ctx = ctx;
        this.servlet = servlet;
        this.request = request;
        response = chunkSize == 0 ? new FullResponse(ctx, request.getHttpRequest())
                : new ChunkedResponse(ctx, request.getHttpRequest(), chunkSize);
        request.getHttpRequest().retain();
    }

    @Override
    public Void call() throws Exception {
        try {
            ChannelFuture lastContentFuture = handle();
            if (!HttpUtil.isKeepAlive(request.getHttpRequest())) {
                lastContentFuture.addListener(ChannelFutureListener.CLOSE);
            }
        } catch (Throwable th) { //NOSONAR
            LOGGER.log(Level.SEVERE, "Failure handling HTTP Request", th);
            ctx.close();
        } finally {
            request.getHttpRequest().release();
        }
        return null;
    }

    private ChannelFuture handle() throws IOException {
        try {
            servlet.handle(request, response);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failure during handling of an IServletRequest", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } finally {
            response.close();
        }
        return response.lastContentFuture();
    }

    public void notifyChannelWritable() {
        response.notifyChannelWritable();
    }

    public void reject() throws IOException {
        try {
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
            response.close();
        } finally {
            request.getHttpRequest().release();
        }
    }

    public IServlet getServlet() {
        return servlet;
    }
}
