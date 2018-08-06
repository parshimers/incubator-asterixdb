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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.logging.LogLevel;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.time.Instant;

//Based in part on LoggingHandler from Netty
public class CLFLogger extends ChannelDuplexHandler {

    protected final InternalLogger logger;
    protected final InternalLogLevel internalLevel;

    StringBuilder logLineBuilder;

    private String clientIp;
    private Instant requestTime;
    private String reqLine;
    private int statusCode;
    private long respSize;
    private String userAgentRef;
    private boolean lastChunk = false;

    public CLFLogger(Class<?> clazz, LogLevel level) {
        logger = InternalLoggerFactory.getInstance(clazz);
        internalLevel = level.toInternalLevel();
        this.logLineBuilder = new StringBuilder();
        respSize = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (logger.isEnabled(internalLevel) && msg instanceof FullHttpRequest) {
            HttpRequest req = (FullHttpRequest) msg;
            clientIp = req.headers().get("Host");
            requestTime = Instant.now();
            reqLine = new StringBuilder().append(req.method().toString()).append(" ").append(req.getUri()).append(" ")
                    .append(req.getProtocolVersion().toString()).append(" ").toString();
            userAgentRef = new StringBuilder().append(headerValueOrDash("Referer", req)).append(" ")
                    .append(headerValueOrDash("User-Agent", req)).toString();
            lastChunk = false;
        }
        ctx.fireChannelRead(msg);
    }

    private String headerValueOrDash(String headerKey, HttpRequest req) {
        String value = req.headers().get(headerKey);
        if (value == null) {
            value = "-";
        }
        return value;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            if (msg instanceof DefaultHttpResponse) {
                HttpResponse resp = (DefaultHttpResponse) msg;
                statusCode = resp.status().code();
                if (msg instanceof DefaultFullHttpResponse) {
                    lastChunk = true;
                    respSize = resp.headers().getInt(HttpHeaderNames.CONTENT_LENGTH);
                }
            } else if (msg instanceof DefaultHttpContent) {
                HttpContent content = (DefaultHttpContent) msg;

                respSize += content.content().readableBytes();
            } else if (msg instanceof LastHttpContent) {
                lastChunk = true;
            }

        }
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (logger.isEnabled(internalLevel) && lastChunk) {
            printAndPrepare();
            lastChunk = false;
        }
        ctx.flush();
    }

    private void printAndPrepare() {
        logLineBuilder.append(clientIp);
        //identd value - not relevant here
        logLineBuilder.append(" - ");
        //no http auth or any auth either for that matter
        logLineBuilder.append(" - [");
        logLineBuilder.append(requestTime);
        logLineBuilder.append("] ");
        logLineBuilder.append(reqLine);
        logLineBuilder.append(" ").append(statusCode);
        logLineBuilder.append(" ").append(respSize);
        logLineBuilder.append(" ").append(userAgentRef);
        logger.log(internalLevel, logLineBuilder.toString());
        respSize = 0;
        logLineBuilder = new StringBuilder();
    }
}
