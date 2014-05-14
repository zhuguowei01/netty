/*
 * Copyright 2013 The Netty Project
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
/*
 * Written by Josh Bloch of Google Inc. and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/.
 */
package io.netty.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * (Transport implementors only) an internal data structure used by {@link AbstractChannel} to store its pending
 * outbound write requests.
 */
public abstract class ChannelOutboundBuffer {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(ChannelOutboundBuffer.class);

    protected AbstractChannel channel;

    private boolean inFail;

    private static final AtomicLongFieldUpdater<ChannelOutboundBuffer> TOTAL_PENDING_SIZE_UPDATER;

    private volatile long totalPendingSize;

    private static final AtomicIntegerFieldUpdater<ChannelOutboundBuffer> WRITABLE_UPDATER;

    static {
        AtomicIntegerFieldUpdater<ChannelOutboundBuffer> writableUpdater =
                PlatformDependent.newAtomicIntegerFieldUpdater(ChannelOutboundBuffer.class, "writable");
        if (writableUpdater == null) {
            writableUpdater = AtomicIntegerFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "writable");
        }
        WRITABLE_UPDATER = writableUpdater;

        AtomicLongFieldUpdater<ChannelOutboundBuffer> pendingSizeUpdater =
                PlatformDependent.newAtomicLongFieldUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
        if (pendingSizeUpdater == null) {
            pendingSizeUpdater = AtomicLongFieldUpdater.newUpdater(ChannelOutboundBuffer.class, "totalPendingSize");
        }
        TOTAL_PENDING_SIZE_UPDATER = pendingSizeUpdater;
    }

    private volatile int writable = 1;

    /**
     * Add the given message to this {@link ChannelOutboundBuffer} so it will be marked as flushed once
     * {@link #addFlush()} was called. The {@link ChannelPromise} will be notified once the write operations
     * completes.
     */
    public final void addMessage(Object msg, ChannelPromise promise) {
        int size = channel.estimatorHandle().size(msg);
        if (size < 0) {
            size = 0;
        }
        addMessage0(msg, size, promise);
        // increment pending bytes after adding message to the unflushed arrays.
        // See https://github.com/netty/netty/issues/1619
        incrementPendingOutboundBytes(size);
    }

    protected abstract void addMessage0(Object msg, int estimatedSize,  ChannelPromise promise);

    /**
     * Mark all messages in this {@link ChannelOutboundBuffer} as flushed.
     */
    public abstract void addFlush();

    /**
     * Increment the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    final void incrementPendingOutboundBytes(int size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue + size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue + size;
        }

        int highWaterMark = channel.config().getWriteBufferHighWaterMark();

        if (newWriteBufferSize > highWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 1, 0)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    /**
     * Decrement the pending bytes which will be written at some point.
     * This method is thread-safe!
     */
    protected final void decrementPendingOutboundBytes(int size) {
        // Cache the channel and check for null to make sure we not produce a NPE in case of the Channel gets
        // recycled while process this method.
        Channel channel = this.channel;
        if (size == 0 || channel == null) {
            return;
        }

        long oldValue = totalPendingSize;
        long newWriteBufferSize = oldValue - size;
        while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
            oldValue = totalPendingSize;
            newWriteBufferSize = oldValue - size;
        }

        int lowWaterMark = channel.config().getWriteBufferLowWaterMark();

        if (newWriteBufferSize == 0 || newWriteBufferSize < lowWaterMark) {
            if (WRITABLE_UPDATER.compareAndSet(this, 0, 1)) {
                channel.pipeline().fireChannelWritabilityChanged();
            }
        }
    }

    protected static long total(Object msg) {
        if (msg instanceof ByteBuf) {
            return ((ByteBuf) msg).readableBytes();
        }
        if (msg instanceof FileRegion) {
            return ((FileRegion) msg).count();
        }
        if (msg instanceof ByteBufHolder) {
            return ((ByteBufHolder) msg).content().readableBytes();
        }
        return -1;
    }

    /**
     * Return current message or {@code null} if no flushed message is left to process.
     */
    public abstract Object current();

    public abstract void progress(long amount);

    /**
     * Mark the current message as successful written and remove it from this {@link ChannelOutboundBuffer}.
     * This method will return {@code true} if there are more messages left to process,  {@code false} otherwise.
     */
    public final boolean remove() {
        if (isEmpty()) {
            return false;
        }

        int size = remove0();
        if (size > 0) {
            decrementPendingOutboundBytes(size);
        }
        return true;
    }

    protected abstract int remove0();

    /**
     * Mark the current message as failure with the given {@link java.lang.Throwable} and remove it from this
     * {@link ChannelOutboundBuffer}. This method will return {@code true} if there are more messages left to process,
     * {@code false} otherwise.
     */
    public final boolean remove(Throwable cause) {
        if (isEmpty()) {
            return false;
        }

        int size = remove0(cause);
        if (size > 0) {
            decrementPendingOutboundBytes(size);
        }
        return true;
    }

    protected abstract int remove0(Throwable cause);

    final boolean getWritable() {
        return writable != 0;
    }

    /**
     * Return the number of messages that are ready to be written (flushed before).
     */
    public abstract int size();

    /**
     * Return {@code true} if this {@link ChannelOutboundBuffer} contains no flushed messages
     */
    public abstract boolean isEmpty();

    /**
     * Fail all previous flushed messages with the given {@link Throwable}.
     */
    final void failFlushed(Throwable cause) {
        // Make sure that this method does not reenter.  A listener added to the current promise can be notified by the
        // current thread in the tryFailure() call of the loop below, and the listener can trigger another fail() call
        // indirectly (usually by closing the channel.)
        //
        // See https://github.com/netty/netty/issues/1501
        if (inFail) {
            return;
        }

        try {
            inFail = true;
            for (;;) {
                if (!remove(cause)) {
                    break;
                }
            }
        } finally {
            inFail = false;
        }
    }

    /**
     * Fail all pending messages with the given {@link ClosedChannelException}.
     */
   final void close(final ClosedChannelException cause) {
        if (inFail) {
            channel.eventLoop().execute(new Runnable() {
                @Override
                public void run() {
                    close(cause);
                }
            });
            return;
        }

        inFail = true;

        if (channel.isOpen()) {
            throw new IllegalStateException("close() must be invoked after the channel is closed.");
        }

        if (!isEmpty()) {
            throw new IllegalStateException("close() must be invoked after all flushed writes are handled.");
        }

        // Release all unflushed messages.
        try {
            int size = failUnflushed(cause);
            long oldValue = totalPendingSize;
            long newWriteBufferSize = oldValue - size;
            while (!TOTAL_PENDING_SIZE_UPDATER.compareAndSet(this, oldValue, newWriteBufferSize)) {
                oldValue = totalPendingSize;
                newWriteBufferSize = oldValue - size;
            }
        } finally {
            inFail = false;
        }

        recycle();
    }

    protected abstract int failUnflushed(ClosedChannelException cause);

    /**
     * Release the message and log if any error happens during release.
     */
    protected static void safeRelease(Object message) {
        try {
            ReferenceCountUtil.release(message);
        } catch (Throwable t) {
            logger.warn("Failed to release a message.", t);
        }
    }

    /**
     * Try to mark the given {@link ChannelPromise} as success and log if this failed.
     */
    protected static void safeSuccess(ChannelPromise promise) {
        if (!(promise instanceof VoidChannelPromise) && !promise.trySuccess()) {
            logger.warn("Failed to mark a promise as success because it is done already: {}", promise);
        }
    }

    /**
     * Try to mark the given {@link ChannelPromise} as failued with the given {@link Throwable} and log if this failed.
     */
    protected static void safeFail(ChannelPromise promise, Throwable cause) {
        if (!(promise instanceof VoidChannelPromise) && !promise.tryFailure(cause)) {
            logger.warn("Failed to mark a promise as failure because it's done already: {}", promise, cause);
        }
    }

    /**
     * Recycle this {@link ChannelOutboundBuffer}. After this was called it is disallowed to use it with the previous
     * assigned {@link AbstractChannel}.
     */
    @SuppressWarnings("unchecked")
    protected void recycle() {
        // Set the channel to null so it can be GC'ed ASAP
        channel = null;

        totalPendingSize = 0;
        writable = 1;
    }

    /**
     * Return the total number of pending bytes.
     */
    public final long totalPendingWriteBytes() {
        return totalPendingSize;
    }

    protected final ByteBuf copyToDirectByteBuf(ByteBuf buf) {
        int readableBytes = buf.readableBytes();
        ByteBufAllocator alloc = channel.alloc();
        if (alloc.isDirectBufferPooled()) {
            ByteBuf directBuf = alloc.directBuffer(readableBytes);
            directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
            safeRelease(buf);
            return directBuf;
        }
        if (ThreadLocalPooledDirectByteBuf.threadLocalDirectBufferSize > 0) {
            ByteBuf directBuf = ThreadLocalPooledDirectByteBuf.newInstance();
            directBuf.writeBytes(buf, buf.readerIndex(), readableBytes);
            safeRelease(buf);
            return directBuf;
        }
        return buf;
    }
}
