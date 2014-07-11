/*
 * Copyright 2014 The Netty Project
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
package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteWriter;
import io.netty.channel.ChannelFlushPromiseNotifier;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.FileRegion;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.TypeParameterMatcher;

import java.nio.ByteBuffer;


/**
 * {@link ChannelOutboundHandlerAdapter} which encodes message in a stream-like fashion.
 * The difference between {@link BufferedMessageToByteEncoder} and {@link MessageToByteEncoder} is
 * that {@link BufferedMessageToByteEncoder}  will try to write multiple writes into one {@link ByteBuf} up to
 * {@code initialBufferCapacity} and only write it to the next {@link ChannelOutboundHandler} in the
 * {@link ChannelPipeline} if needed.
 *
 * You should use this {@link BufferedMessageToByteEncoder} if you expect to either write messages in multiple parts
 * or if the used protocol supports <strong>PIPELINING</strong>.
 *
 * Example implementation which encodes {@link Integer}s to a {@link ByteBuf}.
 *
 * <pre>
 *     public class IntegerEncoder extends {@link BufferedMessageToByteEncoder}&lt;{@link Integer}&gt; {
 *         {@code @Override}
 *         public void encode({@link ChannelHandlerContext} ctx, {@link Integer} msg, {@link Encoder} encoder)
 *                 throws {@link Exception} {
 *             encoder.writeInt(msg);
 *         }
 *     }
 * </pre>
 */
public abstract class BufferedMessageToByteEncoder<I> extends ChannelOutboundHandlerAdapter {

    private final TypeParameterMatcher matcher;
    private final boolean preferDirect;

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private final int initialBufferCapacity;
    private Encoder writer;

    /**
     * @see {@link #BufferedMessageToByteEncoder(int)} with {@value #DEFAULT_BUFFER_SIZE} as parameter.
     */
    protected BufferedMessageToByteEncoder() {
        this(DEFAULT_BUFFER_SIZE);
    }

    /**
     * Create a new instance
     *
     * @param initialBufferCapacity            the size of the buffer when it is allocated
     */
    protected BufferedMessageToByteEncoder(int initialBufferCapacity) {
        this(true, initialBufferCapacity);
    }

    /**
     * Create a new instance
     *
     * @param outboundMessageType   the type of messages to match
     * @param initialBufferCapacity the size of the buffer when it is allocated
     */
    protected BufferedMessageToByteEncoder(Class<? extends I> outboundMessageType, int initialBufferCapacity) {
        this(outboundMessageType, true, initialBufferCapacity);
    }

    /**
     * Create a new instance
     *
     * @param preferDirect          {@code true} if a direct {@link ByteBuf} should be tried to be used as target for
     *                              the encoded messages. If {@code false} is used it will allocate a heap
     *                              {@link ByteBuf}, which is backed by an byte array.
     * @param initialBufferCapacity the size of the buffer when it is allocated.
     */
    protected BufferedMessageToByteEncoder(boolean preferDirect, int initialBufferCapacity) {
        checkSharable();
        checkBufferSize(initialBufferCapacity);
        matcher = TypeParameterMatcher.find(this, BufferedMessageToByteEncoder.class, "I");
        this.initialBufferCapacity = initialBufferCapacity;
        this.preferDirect = preferDirect;
    }

    /**
     * Create a new instance
     *
     * @param outboundMessageType   the type of messages to match
     * @param preferDirect          {@code true} if a direct {@link ByteBuf} should be tried to be used as target for
     *                              the encoded messages. If {@code false} is used it will allocate a heap
     *                              {@link ByteBuf}, which is backed by an byte array.
     * @param initialBufferCapacity the size of the buffer when it is allocated.
     */
    protected BufferedMessageToByteEncoder(
            Class<? extends I> outboundMessageType, boolean preferDirect, int initialBufferCapacity) {
        checkSharable();
        checkBufferSize(initialBufferCapacity);
        matcher = TypeParameterMatcher.get(outboundMessageType);
        this.initialBufferCapacity = initialBufferCapacity;
        this.preferDirect = preferDirect;
    }

    private static void checkBufferSize(int initialBufferCapacity) {
        if (initialBufferCapacity < 0) {
            throw new IllegalArgumentException(
                    "initialBufferCapacity: " + initialBufferCapacity + " (expected: >= 0)");
        }
    }

    private void checkSharable() {
        if (isSharable()) {
            throw new IllegalStateException("@Sharable annotation not allowed");
        }
    }

    /**
     * Returns {@code true} if the given message should be handled. If {@code false} it will be passed to the next
     * {@link ChannelOutboundHandler} in the {@link ChannelPipeline}.
     */
    public boolean acceptOutboundMessage(Object msg) throws Exception {
        return matcher.match(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        try {
            if (acceptOutboundMessage(msg)) {
                @SuppressWarnings("unchecked")
                I cast = (I) msg;

                Encoder writer = this.writer;
                try {
                    writer.init();
                    encode(ctx, cast, writer);
                    writer.notifyLater(promise);
                } finally {
                    ReferenceCountUtil.release(cast);
                }
            } else {
                writer.flush();
                ctx.write(msg, promise);
            }
        } catch (EncoderException e) {
            throw e;
        } catch (Throwable e) {
            throw new EncoderException(e);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        writer.flush();
        super.close(ctx, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        writer.flush();
        super.disconnect(ctx, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        writer.flush();
        super.flush(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        writer = new Encoder(ctx, preferDirect, initialBufferCapacity);
        super.handlerAdded(ctx);
    }

    @Override
    public final void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        writer.flush();
        handlerRemoved0(ctx);
        super.handlerRemoved(ctx);
    }

    /**
     * Do nothing by default, sub-classes may override this method.
     */
    protected void handlerRemoved0(@SuppressWarnings("unused") ChannelHandlerContext ctx) throws Exception {
        // NOOP
    }

    /**
     * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
     * by this encoder.
     *
     * @param ctx           the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param msg           the message to encode
     * @param out           the {@link Encoder} into which the encoded message will be written
     * @throws Exception    is thrown on error
     */
    protected abstract void encode(ChannelHandlerContext ctx, I msg, Encoder out) throws Exception;

    public static final class Encoder implements ByteWriter {
        private final ChannelFlushPromiseNotifier notifier = new ChannelFlushPromiseNotifier();
        private final ChannelHandlerContext ctx;
        private final int initialBufferCapacity;
        private final boolean preferDirect;
        private int writerIndex;
        private ByteBuf wrapped;

        private Encoder(ChannelHandlerContext ctx, boolean preferDirect, int initialBufferCapacity) {
            this.ctx = ctx;
            this.preferDirect = preferDirect;
            this.initialBufferCapacity = initialBufferCapacity;
        }

        void init() {
            if (wrapped == null) {
                wrapped = allocateBuffer();
            }
            writerIndex = wrapped.writerIndex();
        }

        private ByteBuf allocateBuffer() {
            if (preferDirect) {
                if (initialBufferCapacity == 0) {
                    return ctx.alloc().ioBuffer();
                }
                return ctx.alloc().ioBuffer(initialBufferCapacity);
            } else {
                if (initialBufferCapacity == 0) {
                    return ctx.alloc().heapBuffer();
                }
                return ctx.alloc().heapBuffer(initialBufferCapacity);
            }
        }

        @Override
        public Encoder writeBoolean(boolean value) {
            wrapped.writeBoolean(value);
            return this;
        }

        @Override
        public Encoder writeByte(int value) {
            wrapped.writeByte(value);
            return this;
        }

        @Override
        public Encoder writeShort(int value) {
            wrapped.writeShort(value);
            return this;
        }

        @Override
        public Encoder writeMedium(int value) {
            wrapped.writeMedium(value);
            return this;
        }

        @Override
        public Encoder writeInt(int value) {
            wrapped.writeInt(value);
            return this;
        }

        @Override
        public Encoder writeLong(long value) {
            wrapped.writeLong(value);
            return this;
        }

        @Override
        public Encoder writeChar(int value) {
            wrapped.writeChar(value);
            return this;
        }

        @Override
        public Encoder writeFloat(float value) {
            wrapped.writeFloat(value);
            return this;
        }

        @Override
        public Encoder writeDouble(double value) {
            wrapped.writeDouble(value);
            return this;
        }

        @Override
        public Encoder writeBytes(ByteBuf src) {
            wrapped.writeBytes(src);
            return this;
        }

        @Override
        public Encoder writeBytes(ByteBuf src, int length) {
            wrapped.writeBytes(src, length);
            return this;
        }

        @Override
        public Encoder writeBytes(ByteBuf src, int srcIndex, int length) {
            wrapped.writeBytes(src, srcIndex, length);
            return this;
        }

        @Override
        public Encoder writeBytes(byte[] src) {
            wrapped.writeBytes(src);
            return this;
        }

        @Override
        public Encoder writeBytes(byte[] src, int srcIndex, int length) {
            wrapped.writeBytes(src, srcIndex, length);
            return this;
        }

        @Override
        public Encoder writeBytes(ByteBuffer src) {
            wrapped.writeBytes(src);
            return this;
        }

        /**
         * Write {@link FileRegion} to the {@link Encoder} and return itself.
         */
        public Encoder writeFileRegion(FileRegion region) {
            if (region == null) {
                throw new NullPointerException("region");
            }
            // flush now to preserve correct order
            flush();
            ctx.write(region);
            init();
            return this;
        }

        void notifyLater(ChannelPromise promise) {
            long delta = wrapped.writerIndex() - writerIndex;
            ctx.channel().unsafe().outboundBuffer().incrementPendingOutboundBytes(delta);
            // add to notifier so the promise will be notified later once we wrote everything
            notifier.add(promise, delta);
        }

        void flush() {
            ByteBuf buffer = wrapped;
            if (buffer == null) {
                return;
            }
            wrapped = null;
            writerIndex = 0;

            final int size = buffer.readableBytes();
            // Decrement now as we will now trigger the actual write
            ctx.channel().unsafe().outboundBuffer().decrementPendingOutboundBytes(size);

            ctx.write(buffer).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    notifier.increaseWriteCounter(size);
                    if (future.isSuccess()) {
                        notifier.notifyPromises();
                    } else {
                        notifier.notifyPromises(future.cause());
                    }
                }
            });
        }
    }
}
