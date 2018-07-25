package org.apache.hyracks.http.server;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.util.internal.logging.InternalLogLevel;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Instant;

public class CLFLogger extends ChannelDuplexHandler {

    private static final LogLevel DEFAULT_LEVEL = LogLevel.DEBUG;

    protected final InternalLogger logger;
    protected final InternalLogLevel internalLevel;

    private final LogLevel level;
    StringBuilder logLineBuilder;
    SimpleDateFormat timeFormat;

    private String clientIp;
    private Instant requestTime;
    private String reqLine;
    private int statusCode;
    private long respSize;

    public CLFLogger(Class<?> clazz, LogLevel level) {
        if (clazz == null) {
            throw new NullPointerException("clazz");
        }
        if (level == null) {
            throw new NullPointerException("level");
        }

        logger = InternalLoggerFactory.getInstance(clazz);
        this.level = level;
        internalLevel = level.toInternalLevel();
        this.logLineBuilder = new StringBuilder();
        respSize = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            if (msg instanceof FullHttpRequest) {
                HttpRequest req = ((FullHttpRequest) msg);
                clientIp = ctx.channel().remoteAddress().toString();
                requestTime = Instant.now();
                reqLine =
                        new StringBuilder().append(req.method().toString()).append(" ").append(req.getUri().toString())
                                .append(" ").append(req.getProtocolVersion().toString()).append(" ").toString();
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            if (msg instanceof DefaultHttpResponse) {
                HttpResponse resp = ((DefaultHttpResponse) msg);
                statusCode = resp.status().code();
            } else if (msg instanceof DefaultHttpContent) {
                HttpContent content = ((DefaultHttpContent) msg);
                respSize += content.content().readableBytes();
            }

        }
        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (logger.isEnabled(internalLevel)) {
            if (respSize > 0) {
                printAndPrepare();
            }
        }
        ctx.flush();
    }

    private void printAndPrepare() {
        logLineBuilder.append(clientIp);
        logLineBuilder.append(" - ");
        logLineBuilder.append(" - [");
        logLineBuilder.append(requestTime);
        logLineBuilder.append("] ");
        logLineBuilder.append(reqLine);
        logLineBuilder.append(" ").append(statusCode);
        logLineBuilder.append(" ").append(respSize);
        logger.log(internalLevel, logLineBuilder.toString());
        respSize = 0;
        logLineBuilder = new StringBuilder();
    }
}
