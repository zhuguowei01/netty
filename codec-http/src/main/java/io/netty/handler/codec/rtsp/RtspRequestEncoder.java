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
package io.netty.handler.codec.rtsp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteWriter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.CharsetUtil;

import static io.netty.handler.codec.http.HttpConstants.*;

/**
 * Encodes an RTSP request represented in {@link FullHttpRequest} into
 * a {@link ByteBuf}.

 */
public class RtspRequestEncoder extends RtspObjectEncoder<HttpRequest> {
    private static final byte[] CRLF = { CR, LF };

    @Override
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return msg instanceof FullHttpRequest;
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void encodeInitialLine(ByteWriter encoder, HttpRequest request) throws Exception {
        HttpHeaders.encodeAscii(request.getMethod().toString(), encoder);
        encoder.writeByte(SP);
        encoder.writeBytes(request.getUri().getBytes(CharsetUtil.UTF_8));
        encoder.writeByte(SP);
        encodeAscii(request.getProtocolVersion().toString(), encoder);
        encoder.writeBytes(CRLF);
    }
}
