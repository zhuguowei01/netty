/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteWriter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.BufferedMessageToByteEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.StringUtil;

import static io.netty.handler.codec.http.HttpConstants.*;

/**
 * Encodes an {@link HttpMessage} or an {@link HttpContent} into
 * a {@link ByteBuf}.
 *
 * <h3>Extensibility</h3>
 *
 * Please note that this encoder is designed to be extended to implement
 * a protocol derived from HTTP, such as
 * <a href="http://en.wikipedia.org/wiki/Real_Time_Streaming_Protocol">RTSP</a> and
 * <a href="http://en.wikipedia.org/wiki/Internet_Content_Adaptation_Protocol">ICAP</a>.
 * To implement the encoder of such a derived protocol, extend this class and
 * implement all abstract methods properly.
 */
public abstract class HttpObjectEncoder<H extends HttpMessage> extends BufferedMessageToByteEncoder<Object> {
    private static final byte[] CRLF = { CR, LF };
    private static final byte[] ZERO_CRLF = { '0', CR, LF };
    private static final byte[] ZERO_CRLF_CRLF = { '0', CR, LF, CR, LF };

    private static final int ST_INIT = 0;
    private static final int ST_CONTENT_NON_CHUNK = 1;
    private static final int ST_CONTENT_CHUNK = 2;

    @SuppressWarnings("RedundantFieldInitialization")
    private int state = ST_INIT;

    protected HttpObjectEncoder() { }

    protected HttpObjectEncoder(int bufferSize) {
        super(bufferSize);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, Encoder encoder) throws Exception {
        if (msg instanceof HttpMessage) {
            if (state != ST_INIT) {
                throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
            }

            @SuppressWarnings({ "unchecked", "CastConflictsWithInstanceof" })
            H m = (H) msg;

            // Encode the message.
            encodeInitialLine(encoder, m);
            HttpHeaders.encode(m.headers(), encoder);
            encoder.writeBytes(CRLF);
            state = HttpHeaders.isTransferEncodingChunked(m) ? ST_CONTENT_CHUNK : ST_CONTENT_NON_CHUNK;
        }
        if (msg instanceof HttpContent || msg instanceof ByteBuf || msg instanceof FileRegion) {
            if (state == ST_INIT) {
                throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
            }

            final long contentLength = contentLength(msg);
            if (state == ST_CONTENT_NON_CHUNK) {
                if (contentLength > 0) {
                    encodeMsg(encoder, msg);
                }

                if (msg instanceof LastHttpContent) {
                    state = ST_INIT;
                }
            } else if (state == ST_CONTENT_CHUNK) {

                encodeChunkedContent(msg, contentLength, encoder);
            } else {
                throw new Error();
            }
        }
    }

    private void encodeChunkedContent(Object msg, long contentLength, Encoder encoder) {
        if (contentLength > 0) {
            byte[] length = Long.toHexString(contentLength).getBytes(CharsetUtil.US_ASCII);
            encoder.writeBytes(length);
            encoder.writeBytes(CRLF);
            encodeMsg(encoder, msg);
            encoder.writeBytes(CRLF);
        }

        if (msg instanceof LastHttpContent) {
            HttpHeaders headers = ((LastHttpContent) msg).trailingHeaders();
            if (headers.isEmpty()) {
                encoder.writeBytes(ZERO_CRLF_CRLF);
            } else {
                encoder.writeBytes(ZERO_CRLF);
                HttpHeaders.encode(headers, encoder);
                encoder.writeBytes(CRLF);
            }

            state = ST_INIT;
        }
    }

    private static void encodeMsg(Encoder encoder, Object msg) {
        if (msg instanceof HttpContent) {
            encoder.writeBytes(((HttpContent) msg).content());
        } else if (msg instanceof FileRegion) {
            encoder.writeFileRegion((FileRegion) msg);
        } else {
            // TODO: Fix me
            throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
        }
    }

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return msg instanceof HttpObject || msg instanceof ByteBuf || msg instanceof FileRegion;
    }

    private static long contentLength(Object msg) {
        if (msg instanceof HttpContent) {
            return ((HttpContent) msg).content().readableBytes();
        }
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        throw new IllegalStateException("unexpected message type: " + StringUtil.simpleClassName(msg));
    }

    @Deprecated
    protected static void encodeAscii(String s, ByteWriter buf) {
        HttpHeaders.encodeAscii0(s, buf);
    }

    protected abstract void encodeInitialLine(ByteWriter encoder, H message) throws Exception;
}
