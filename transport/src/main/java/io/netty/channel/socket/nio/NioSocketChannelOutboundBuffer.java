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
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelFlushPromiseNotifier.FlushCheckpoint;
import io.netty.util.Recycler;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Arrays;

/**
 * Special {@link ChannelOutboundBuffer} implementation which allows to also access flushed {@link ByteBuffer} to
 * allow efficent gathering writes.
 */
public final class NioSocketChannelOutboundBuffer extends ChannelOutboundBuffer {
    protected static final int INITIAL_CAPACITY = 32;
    private final Recycler.Handle<NioSocketChannelOutboundBuffer> handle;
    private ByteBuffer[] nioBuffers;
    private int nioBufferCount;
    private long nioBufferSize;
    private long totalPending;
    private long writeCounter;

    private Entry first;
    private Entry last;
    private int flushed;
    private int messages;

    private final ArrayDeque<FlushCheckpoint> promises =
            new ArrayDeque<FlushCheckpoint>();

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
        nioBuffers = new ByteBuffer[INITIAL_CAPACITY];
    }

    /**
     * Add the given message to this {@link ChannelOutboundBuffer} so it will be marked as flushed once
     * {@link #addFlush()} was called. The {@link io.netty.channel.ChannelPromise} will be notified once the write operations
     * completes.
     */
    @Override
    public int add(Object msg, ChannelPromise promise) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (!buf.isDirect()) {
                msg = copyToDirectByteBuf(buf);
            }
        }
        long total = total(msg);
        int size = (int) total(msg);

        Entry e = Entry.newInstance(this);
        if (last == null) {
            first = e;
            last = e;
        } else {
            last.next = e;
            last = e;
        }
        e.msg = msg;
        e.pendingSize = size;
        e.pendingTotal = total;
        totalPending += total;
        addPromise(promise);

        messages++;
        return size;
    }

    private void addPromise(ChannelPromise promise) {
        if (isVoidPromise(promise)) {
            // no need to add the promises to later notify if it is a VoidChannelPromise
            return;
        }
        FlushCheckpoint checkpoint;
        if (promise instanceof FlushCheckpoint) {
            checkpoint = (FlushCheckpoint) promise;
            checkpoint.flushCheckpoint(totalPending);
        } else {
            checkpoint = new DefaultFlushCheckpoint(totalPending, promise);
        }

        promises.offer(checkpoint);
    }

    @Override
    public void addFlush() {
        flushed = messages;
        // TODO: Fix me
        /*
        final int mask = buffer.length - 1;
        int i = flushed;
        while (i != unflushed && buffer[i].msg != null) {
            Entry entry = buffer[i];
            if (!entry.promise.setUncancellable()) {
                // Was cancelled so make sure we free up memory and notify about the freed bytes
                int pending = entry.cancel();
                decrementPendingOutboundBytes(pending, false);
            }
            i = i + 1 & mask;
        }
        */
    }

    @Override
    public Object current() {
        if (isEmpty()) {
            return null;
        } else {
            return first.msg;
        }
    }

    @Override
    public void progress(long amount) {
        Entry e = first;
        e.pendingTotal -= amount;
        assert e.pendingTotal >= 0;
        if (amount > 0) {
            writeCounter += amount;
            notifyPromises(null);
        }
    }

    @Override
    public int remove0() {
        Entry e = first;
        first = e.next;
        if (first == null) {
            last = null;
        }

        messages--;
        flushed--;
        return e.success();
    }

    @Override
    public int remove0(Throwable cause) {
        Entry e = first;
        first = e.next;
        if (first == null) {
            last = null;
        }

        messages--;
        flushed--;
        return e.fail(cause, true);
    }

    @Override
    public int size() {
        return flushed;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    protected void failUnflushed(final ClosedChannelException cause) {
        Entry e = first;
        while (e != null) {
            e.fail(cause, false);
            e = e.next;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void recycle() {
        super.recycle();
        assert promises.isEmpty();
        nioBufferCount = 0;
        nioBufferSize = 0;
        totalPending = 0;
        writeCounter = 0;
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
        private static final Recycler<Entry> RECYCLER = new Recycler<Entry>() {
            @Override
            protected Entry newObject(Handle<Entry> handle) {
                return new Entry(handle);
            }
        };

        static Entry newInstance(NioSocketChannelOutboundBuffer buffer) {
            Entry entry = RECYCLER.get();
            entry.buffer = buffer;
            return entry;
        }

        private final Recycler.Handle<Entry> entryHandle;
        boolean cancelled;
        ByteBuffer[] buffers;
        ByteBuffer buf;
        int count = -1;
        private Object msg;
        private int pendingSize;
        private long pendingTotal;

        private Entry next;
        private NioSocketChannelOutboundBuffer buffer;

        @SuppressWarnings("unchecked")
        private Entry(Recycler.Handle<? extends Entry> entryHandle) {
            this.entryHandle = (Recycler.Handle<Entry>) entryHandle;
        }

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
            if (!cancelled) {
                cancelled = true;
                int pSize = pendingSize;

                // release message and replace with an empty buffer
                safeRelease(msg);
                msg = Unpooled.EMPTY_BUFFER;
                pendingTotal = 0;
                pendingSize = 0;
                buffers = null;
                buf = null;
                return pSize;
            }
            return 0;
        }

        private int success() {
            try {
                buffer.writeCounter += pendingTotal;
                if (!cancelled) {
                    safeRelease(msg);
                    buffer.notifyPromises(null);
                }
                return pendingSize;
            } finally {
                recycle();
            }
        }

        private int fail(Throwable cause, boolean decrementAndNotify) {
            cause.printStackTrace();
            try {
                buffer.writeCounter += pendingTotal;
                if (!cancelled) {
                    safeRelease(msg);
                    buffer.notifyPromises(cause);
                    if (decrementAndNotify) {
                        return pendingSize;
                    }
                }
                return 0;
            } finally {
                recycle();
            }
        }

        private void recycle() {
            msg = null;
            pendingSize = 0;
            pendingTotal = 0;
            next = null;
            buffer = null;
            buffers = null;
            buf = null;
            count = -1;
            entryHandle.recycle(this);
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

        if (!isEmpty()) {
            ByteBuffer[] nioBuffers = this.nioBuffers;
            Entry entry = first;
            int i = size();

            for (;;) {
                Object m = entry.msg;
                if (!(m instanceof ByteBuf)) {
                    this.nioBufferCount = 0;
                    this.nioBufferSize = 0;
                    return null;
                }

                ByteBuf buf = (ByteBuf) m;
                final int readerIndex = buf.readerIndex();
                final int readableBytes = buf.writerIndex() - readerIndex;

                if (readableBytes > 0) {
                    nioBufferSize += readableBytes;
                    int count = entry.count;
                    if (count == -1) {
                        //noinspection ConstantValueVariableUse
                        entry.count = count = buf.nioBufferCount();
                    }
                    int neededSpace = nioBufferCount + count;
                    if (neededSpace > nioBuffers.length) {
                        this.nioBuffers = nioBuffers = expandNioBufferArray(nioBuffers, neededSpace, nioBufferCount);
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
                            // cached ByteBuffers as they may be expensive to create in terms of Object allocation
                            entry.buffers = nioBufs = buf.nioBuffers();
                        }
                        nioBufferCount = fillBufferArray(nioBufs, nioBuffers, nioBufferCount);
                    }
                }
                if (--i == 0) {
                    break;
                }
                entry = entry.next;
            }
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

    private void notifyPromises(Throwable cause) {
        final long writeCounter = this.writeCounter;
        for (;;) {
            FlushCheckpoint cp = promises.peek();
            if (cp == null) {
                // Reset the counter if there's nothing in the notification list.
                this.writeCounter = 0;
                totalPending = 0;
                break;
            }

            if (cp.flushCheckpoint() > writeCounter) {
                if (writeCounter > 0 && promises.size() == 1) {
                    this.writeCounter = 0;
                    totalPending -= writeCounter;
                    cp.flushCheckpoint(cp.flushCheckpoint() - writeCounter);
                }
                break;
            }

            promises.remove();
            if (cause == null) {
                cp.promise().trySuccess();
            } else {
                safeFail(cp.promise(), cause);
            }
        }
        // Avoid overflow
        final long newWriteCounter = this.writeCounter;
        if (newWriteCounter >= 0x8000000000L) {
            // Reset the counter only when the counter grew pretty large
            // so that we can reduce the cost of updating all entries in the notification list.
            this.writeCounter = 0;
            totalPending -= newWriteCounter;
            for (FlushCheckpoint cp: promises) {
                cp.flushCheckpoint(cp.flushCheckpoint() - newWriteCounter);
            }
        }
    }

    private static class DefaultFlushCheckpoint implements FlushCheckpoint {
        private long checkpoint;
        private final ChannelPromise future;

        DefaultFlushCheckpoint(long checkpoint, ChannelPromise future) {
            this.checkpoint = checkpoint;
            this.future = future;
        }

        @Override
        public long flushCheckpoint() {
            return checkpoint;
        }

        @Override
        public void flushCheckpoint(long checkpoint) {
            this.checkpoint = checkpoint;
        }

        @Override
        public ChannelPromise promise() {
            return future;
        }
    }
}
