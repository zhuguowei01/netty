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
import io.netty.channel.AbstractChannel;
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
    private static final int INITIAL_CAPACITY = 128;

    private final Recycler.Handle<NioSocketChannelOutboundBuffer> handle;

    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;

    // A circular buffer used to store messages.  The buffer is arranged such that:  flushed <= unflushed <= tail.  The
    // flushed messages are stored in the range [flushed, unflushed).  Unflushed messages are stored in the range
    // [unflushed, tail).
    private MessageEntry[] buffer;
    private int flushed;
    private int unflushed;
    private int tail;

    private ChannelPromiseEntry[] promises;
    private int promisesTail;
    private int promisesHead;

    private long writeCounter;

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

    private NioSocketChannelOutboundBuffer(Recycler.Handle<NioSocketChannelOutboundBuffer> handle) {
        this.handle = handle;

        buffer = new MessageEntry[INITIAL_CAPACITY];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = new MessageEntry();
        }

        promises = new ChannelPromiseEntry[INITIAL_CAPACITY];
        for (int i = 0; i < promises.length; i++) {
            promises[i] = new ChannelPromiseEntry();
        }

        nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
    }

    @Override
    public void addMessage0(Object msg, int estimatedSize, ChannelPromise promise) {
        if (msg instanceof ByteBuf) {
            // TODO: Merge buffers if configured
            ByteBuf buf = (ByteBuf) msg;
            if (!buf.isDirect()) {
                msg = copyToDirectByteBuf(buf);
            }
        }

        MessageEntry e = buffer[tail++];
        e.msg = msg;
        e.pendingSize = estimatedSize;

        tail &= buffer.length - 1;

        if (tail == flushed) {
            addCapacity();
        }

        ChannelPromiseEntry promiseEntry = promises[promisesTail++];
        promiseEntry.promise = promise;
        promiseEntry.checkpoint = writeCounter;
        writeCounter += total(msg);
        promiseEntry.total = writeCounter;

        promisesTail &= promises.length - 1;
        if (promisesTail == promisesHead) {
            addPromisesCapacity();
        }
    }

    /**
     * Expand internal array which holds the {@link MessageEntry}'s.
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

        MessageEntry[] e = new MessageEntry[newCapacity];
        System.arraycopy(buffer, p, e, 0, r);
        System.arraycopy(buffer, 0, e, r, p);
        for (int i = n; i < e.length; i++) {
            e[i] = new MessageEntry();
        }

        buffer = e;
        flushed = 0;
        unflushed = s;
        tail = n;
    }

    /**
     * Expand internal array which holds the {@link MessageEntry}'s.
     */
    private void addPromisesCapacity() {
        int p = promisesHead;
        int n = promises.length;
        int r = n - p; // number of elements to the right of p

        int newCapacity = n << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException();
        }

        ChannelPromiseEntry[] e = new ChannelPromiseEntry[newCapacity];
        System.arraycopy(promises, p, e, 0, r);
        System.arraycopy(promises, 0, e, r, p);
        for (int i = n; i < e.length; i++) {
            e[i] = new ChannelPromiseEntry();
        }

        promises = e;
        promisesHead = 0;
        promisesTail = n;
    }

    @Override
    public void addFlush() {
        unflushed = tail;
    }

    @Override
    protected Object current0() {
        MessageEntry entry = buffer[flushed];
        return entry.msg;
    }

    @Override
    public void progress(long amount) {
        MessageEntry entry = buffer[flushed];
        entry.pendingSize -= amount;

        // TODO: Think about it
        decrementPendingOutboundBytes((int) amount);

        notifyPromises(amount);
    }

    @Override
    protected int remove0() {
        MessageEntry e = removeEntry();
        if (e == null) {
            return -1;
        }
        flushed = flushed + 1 & buffer.length - 1;

        int size = e.clear();
        if (size > 0) {
            notifyPromises(size);
        }
        return size;
    }

    @Override
    protected int remove0(Throwable cause) {
        MessageEntry e = removeEntry();
        if (e == null) {
            return -1;
        }
        flushed = flushed + 1 & buffer.length - 1;

        int size = e.clear();
        if (size > 0) {
            failPromises(size, cause);
        }
        return size;
    }

    private long notifyPromises(long amount) {
        for (;;) {
            ChannelPromiseEntry e = promises[promisesHead];
            if (e.promise == null) {
                break;
            }
            long bytes = e.total - e.checkpoint;
            if (amount >= bytes) {
                amount -= bytes;
                promisesHead = promisesHead + 1 & promises.length - 1;
                e.success();
            } else {
                e.checkpoint += bytes;
                break;
            }
        }
        return amount;
    }

    private long failPromises(long amount, Throwable cause) {
        for (;;) {
            ChannelPromiseEntry pe = promises[promisesHead];
            if (pe.promise == null) {
                break;
            }
            long bytes = pe.total - pe.checkpoint;
            if (amount >= bytes) {
                amount -= bytes;
                promisesHead = promisesHead + 1 & promises.length - 1;
                pe.fail(cause);
            } else {
                pe.checkpoint += bytes;
                break;
            }
        }
        return amount;
    }

    private MessageEntry removeEntry() {
        MessageEntry e = buffer[flushed];
        Object msg = e.msg;
        if (msg == null) {
            return null;
        }
        return e;
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
        int size = 0;
        // Release all unflushed messages.
        final int unflushedCount = tail - unflushed & buffer.length - 1;
        try {
            for (int i = 0; i < unflushedCount; i++) {
                MessageEntry e = buffer[unflushed + i & buffer.length - 1];
                size += e.clear();
            }
        } finally {
            tail = unflushed;
        }
        failPromises(size, cause);
        return size;
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
        final MessageEntry[] buffer = this.buffer;
        final int mask = buffer.length - 1;
        ByteBuffer[] nioBuffers = this.nioBuffers;
        Object m;
        int unflushed = this.unflushed;
        int i = flushed;
        while (i != unflushed && (m = buffer[i].msg) != null) {
            if (!(m instanceof ByteBuf)) {
                this.nioBufferCount = 0;
                this.nioBufferSize = 0;
                return null;
            }

            MessageEntry entry = buffer[i];
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

    @Override
    public void recycle() {
        // take care of recycle the ByteBuffer[] structure.
        if (nioBuffers.length > INITIAL_CAPACITY) {
            nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
        } else {
            // null out the nio buffers array so the can be GC'ed
            // https://github.com/netty/netty/issues/1763
            Arrays.fill(nioBuffers, null);
        }
        if (buffer.length > INITIAL_CAPACITY) {
            MessageEntry[] e = new MessageEntry[INITIAL_CAPACITY];
            System.arraycopy(buffer, 0, e, 0, INITIAL_CAPACITY);
            buffer = e;
        }
        // reset flushed, unflushed and tail
        // See https://github.com/netty/netty/issues/1772
        flushed = 0;
        unflushed = 0;
        tail = 0;

        if (promises.length > INITIAL_CAPACITY) {
            ChannelPromiseEntry[] e = new ChannelPromiseEntry[INITIAL_CAPACITY];
            System.arraycopy(promises, 0, e, 0, INITIAL_CAPACITY);
            promises = e;
        }
        promisesHead = 0;
        promisesTail = 0;

        RECYCLER.recycle(this, handle);
        super.recycle();
    }

    private static class MessageEntry {
        Object msg;
        long progress;
        int pendingSize;
        ByteBuffer[] buffers;
        ByteBuffer buf;
        int count = -1;

        /**
         * Clear this {@link MessageEntry} and so release all resources.
         */
        public int clear() {
            int size = pendingSize;
            safeRelease(msg);

            msg = null;
            progress = 0;
            pendingSize = 0;
            buffers = null;
            buf = null;
            count = -1;
            return size;
        }
    }

    private static final class ChannelPromiseEntry {
        ChannelPromise promise;
        long checkpoint;
        long total;

        public void fail(Throwable cause) {
            safeFail(promise, cause);
            clear();
        }

        public void success() {
            safeSuccess(promise);
            clear();
        }

        private void clear() {
            promise = null;
            checkpoint = 0;
            total = 0;
        }
    }
}
