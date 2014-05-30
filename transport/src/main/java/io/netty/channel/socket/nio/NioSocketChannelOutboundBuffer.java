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
/*
 * Written by Josh Bloch of Google Inc. and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/.
 */
package io.netty.channel.socket.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AbstractChannel;
import io.netty.channel.ChannelFlushPromiseNotifier;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.util.Recycler;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

/**
 * Special {@link ChannelOutboundBuffer} implementation which allows to also access flushed {@link ByteBuffer} to
 * allow efficent gathering writes.
 */
public final class NioSocketChannelOutboundBuffer extends ChannelOutboundBuffer {

    private static final Recycler<NioSocketChannelOutboundBuffer> RECYCLER =
            new Recycler<NioSocketChannelOutboundBuffer>() {
        @Override
        protected NioSocketChannelOutboundBuffer newObject(Handle<NioSocketChannelOutboundBuffer> handle) {
            return new NioSocketChannelOutboundBuffer(handle);
        }
    };

    /**
     * Get a new instance of this {@link NioSocketChannelOutboundBuffer} and attach it the given {@link AbstractChannel}
     */
    public static NioSocketChannelOutboundBuffer newInstance(AbstractChannel channel) {
        NioSocketChannelOutboundBuffer buffer = RECYCLER.get();
        buffer.channel = channel;
        return buffer;
    }

    protected static final int INITIAL_CAPACITY = 32;

    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;

    private final Recycler.Handle<NioSocketChannelOutboundBuffer> handle;
    private final ChannelFlushPromiseNotifier promiseNotifier;

    // A circular buffer used to store messages.  The buffer is arranged such that:  flushed <= unflushed <= tail.  The
    // flushed messages are stored in the range [flushed, unflushed).  Unflushed messages are stored in the range
    // [unflushed, tail).
    private Entry[] buffer;
    private int flushed;
    private int unflushed;
    private int tail;

    private Entry lastEntry;

    protected NioSocketChannelOutboundBuffer(Recycler.Handle<NioSocketChannelOutboundBuffer> handle) {
        this.handle = handle;

        buffer = new Entry[INITIAL_CAPACITY];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = new Entry();
        }
        nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
        promiseNotifier = new ChannelFlushPromiseNotifier(true, INITIAL_CAPACITY);
    }

    @Override
    protected void addMessage0(Object msg, int estimatedSize, ChannelPromise promise) {
        promiseNotifier.add(promise, estimatedSize);
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            /*if (lastEntry != null && !lastEntry.flushed && lastEntry.msg instanceof ByteBuf) {
                ByteBuf lastBuf = (ByteBuf) lastEntry.msg;
                if (lastBuf.isWritable(buf.readableBytes())) {
                    lastBuf.writeBytes(buf);
                    safeRelease(buf);
                    return;
                }
            }*/
            if (!buf.isDirect()) {
                msg = copyToDirectByteBuf(buf);
            }
        }
        Entry e = buffer[tail++];
        e.msg = msg;
        e.pendingSize = estimatedSize;
        e.total = total(msg);
        tail &= buffer.length - 1;

        if (tail == flushed) {
            addCapacity();
        }

        lastEntry = e;
    }

    /**
     * Expand internal array which holds the {@link Entry}'s.
     */
    private void addCapacity() {
        int p = flushed;
        int n = buffer.length;
        int r = n - p; // number of elements to the right of p
        int s = size();

        int newCapacity = n << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException();
        }

        Entry[] e = new Entry[newCapacity];
        System.arraycopy(buffer, p, e, 0, r);
        System.arraycopy(buffer, 0, e, r, p);
        for (int i = n; i < e.length; i++) {
            e[i] = new Entry();
        }

        buffer = e;
        flushed = 0;
        unflushed = s;
        tail = n;
    }

    @Override
    public void addFlush() {
        unflushed = tail;

        final int mask = buffer.length - 1;
        int i = flushed;
        while (i != unflushed && buffer[i].msg != null) {
            Entry entry = buffer[i];
            entry.flushed = true;
            /*ChannelPromise promise = promises[i].promise();
            if (!promise.setUncancellable()) {
                // Was cancelled so make sure we free up memory and notify about the freed bytes
                int pending = entry.cancel();
                decrementPendingOutboundBytes(pending);
            }*/
            i = i + 1 & mask;
        }
    }

    @Override
    public Object current() {
        if (isEmpty()) {
            return null;
        } else {
            Entry entry = buffer[flushed];
            return entry.msg;
        }
    }

    @Override
    public void progress(long amount) {
        increaseAndNotify(amount, null);
        Entry e = buffer[flushed];
        long progress = e.progress + amount;
        e.progress = progress;
    }

    private void increaseAndNotify(long amount, Throwable cause) {
        if (amount > 0) {
            promiseNotifier.increaseWriteCounter(amount);
            if (cause == null) {
                promiseNotifier.notifyFlushFutures();
            } else {
                promiseNotifier.notifyFlushFutures(cause);
            }
        }
    }

    @Override
    protected int remove0() {
        Entry e = buffer[flushed];
        Object msg = e.msg;
        int size = e.pendingSize;

        e.clear();

        flushed = flushed + 1 & buffer.length - 1;

        increaseAndNotify(size, null);
        if (!e.cancelled) {
            // only release message, notify and decrement if it was not canceled before.
            safeRelease(msg);
            return size;
        }

        return 0;
    }

    @Override
    protected int remove0(Throwable cause) {
        Entry e = buffer[flushed];
        Object msg = e.msg;
        int size = e.pendingSize;

        e.clear();

        flushed = flushed + 1 & buffer.length - 1;

        increaseAndNotify(size, cause);

        if (!e.cancelled) {
            // only release message, fail and decrement if it was not canceled before.
            safeRelease(msg);

            promiseNotifier.notifyFlushFutures(cause);
            return size;
        }

        return 0;
    }

    @Override
    public int size() {
        return unflushed - flushed & buffer.length - 1;
    }

    @Override
    public boolean isEmpty() {
        return unflushed == flushed;
    }

    @Override
    protected int failUnflushed(ClosedChannelException cause) {
        // Release all unflushed messages.
        final int unflushedCount = tail - unflushed & buffer.length - 1;
        try {
            int size = 0;
            for (int i = 0; i < unflushedCount; i++) {
                int index = unflushed + i & buffer.length - 1;
                Entry e = buffer[index];

                size += e.pendingSize;

                e.pendingSize = 0;
                if (!e.cancelled) {
                    safeRelease(e.msg);
                    //safeFail(promises[index].promise(), cause);
                }
                e.msg = null;
            }
            promiseNotifier.notifyFlushFutures(cause);
            return size;
        } finally {
            tail = unflushed;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void recycle() {
        super.recycle();
        if (buffer.length > INITIAL_CAPACITY) {
            Entry[] e = new Entry[INITIAL_CAPACITY];
            System.arraycopy(buffer, 0, e, 0, INITIAL_CAPACITY);
            buffer = e;
        }

        // reset flushed, unflushed and tail
        // See https://github.com/netty/netty/issues/1772
        flushed = 0;
        unflushed = 0;
        tail = 0;

        promiseNotifier.reset();

        // take care of recycle the ByteBuffer[] structure.
        if (nioBuffers.length > INITIAL_CAPACITY) {
            nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
        } else {
            // null out the nio buffers array so the can be GC'ed
            // https://github.com/netty/netty/issues/1763
            Arrays.fill(nioBuffers, null);
        }

        RECYCLER.recycle(this, handle);
    }

    protected static class Entry {
        ByteBuffer[] buffers;
        ByteBuffer buf;
        Object msg;
        long progress;
        long total;
        int pendingSize;
        int count = -1;
        boolean cancelled;
        boolean flushed;

        public Object msg() {
            return msg;
        }

        /**
         * Return {@code true} if the {@link Entry} was cancelled via {@link #cancel()} before,
         * {@code false} otherwise.
         */
        public boolean isCancelled() {
            return cancelled;
        }

        /**
         * Cancel this {@link Entry} and the message that was hold by this {@link Entry}. This method returns the
         * number of pending bytes for the cancelled message.
         */
        public int cancel() {
            buffers = null;
            buf = null;
            count = -1;

            if (!cancelled) {
                cancelled = true;
                int pSize = pendingSize;

                // release message and replace with an empty buffer
                safeRelease(msg);
                msg = Unpooled.EMPTY_BUFFER;

                pendingSize = 0;
                total = 0;
                progress = 0;
                return pSize;
            }
            return 0;
        }

        /**
         * Clear this {@link Entry} and so release all resources.
         */
        public void clear() {
            buffers = null;
            buf = null;
            count = -1;
            msg = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = -1;
            cancelled = false;
            flushed = false;
        }
    }

    /**
     * Returns an array of direct NIO buffers if the currently pending messages are made of {@link ByteBuf} only.
     * {@code null} is returned otherwise.  If this method returns a non-null array, {@link #nioBufferCount()} and
     * {@link #nioBufferSize()} will return the number of NIO buffers in the returned array and the total number
     * of readable bytes of the NIO buffers respectively.
     * <p>
     * Note that the returned array is reused and thus should not escape
     * {@link io.netty.channel.AbstractChannel#doWrite(ChannelOutboundBuffer)}.
     * Refer to {@link io.netty.channel.socket.nio.NioSocketChannel#doWrite(ChannelOutboundBuffer)} for an example.
     * </p>
     */
    public ByteBuffer[] nioBuffers() {
        long nioBufferSize = 0;
        int nioBufferCount = 0;
        final Entry[] buffer = this.buffer;
        final int mask = buffer.length - 1;
        ByteBuffer[] nioBuffers = this.nioBuffers;
        Object m;
        int unflushed = this.unflushed;
        int i = flushed;
        while (i != unflushed && (m = buffer[i].msg()) != null) {
            if (!(m instanceof ByteBuf)) {
                this.nioBufferCount = 0;
                this.nioBufferSize = 0;
                return null;
            }

            Entry entry = buffer[i];

            if (!entry.isCancelled()) {
                ByteBuf buf = (ByteBuf) m;
                final int readerIndex = buf.readerIndex();
                final int readableBytes = buf.writerIndex() - readerIndex;

                if (readableBytes > 0) {
                    nioBufferSize += readableBytes;
                    int count = entry.count;
                    if (count == -1) {
                        //noinspection ConstantValueVariableUse
                        entry.count = count =  buf.nioBufferCount();
                    }
                    int neededSpace = nioBufferCount + count;
                    if (neededSpace > nioBuffers.length) {
                        this.nioBuffers = nioBuffers =
                                expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
                    }
                    if (count == 1) {
                        ByteBuffer nioBuf = entry.buf;
                        if (nioBuf == null) {
                            // cache ByteBuffer as it may need to create a new ByteBuffer instance if its a
                            // derived buffer
                            entry.buf = nioBuf = buf.internalNioBuffer(readerIndex, readableBytes);
                        }
                        nioBuffers[nioBufferCount ++] = nioBuf;
                    } else {
                        ByteBuffer[] nioBufs = entry.buffers;
                        if (nioBufs == null) {
                            // cached ByteBuffers as they may be expensive to create in terms
                            // of Object allocation
                            entry.buffers = nioBufs = buf.nioBuffers();
                        }
                        nioBufferCount = fillBufferArray(nioBufs, nioBuffers, nioBufferCount);
                    }
                }
            }

            i = i + 1 & mask;
        }
        this.nioBufferCount = nioBufferCount;
        this.nioBufferSize = nioBufferSize;

        return nioBuffers;
    }

    private static int fillBufferArray(ByteBuffer[] nioBufs, ByteBuffer[] nioBuffers, int nioBufferCount) {
        for (ByteBuffer nioBuf: nioBufs) {
            if (nioBuf == null) {
                break;
            }
            nioBuffers[nioBufferCount ++] = nioBuf;
        }
        return nioBufferCount;
    }

    private static ByteBuffer[] expandNioBufferArray(ByteBuffer[] array, int neededSpace, int size) {
        int newCapacity = array.length;
        do {
            // double capacity until it is big enough
            // See https://github.com/netty/netty/issues/1890
            newCapacity <<= 1;

            if (newCapacity < 0) {
                throw new IllegalStateException();
            }

        } while (neededSpace > newCapacity);

        ByteBuffer[] newArray = new ByteBuffer[newCapacity];
        System.arraycopy(array, 0, newArray, 0, size);

        return newArray;
    }

    /**
     * Return the number of {@link java.nio.ByteBuffer} which can be written.
     */
    public int nioBufferCount() {
        return nioBufferCount;
    }

    /**
     * Return the number of bytes that can be written via gathering writes.
     */
    public long nioBufferSize() {
        return nioBufferSize;
    }
}
