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

import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;

import java.nio.channels.ClosedChannelException;

public class DefaultChannelOutboundBuffer extends ChannelOutboundBuffer {

    protected static final int INITIAL_CAPACITY = 32;

    private static final Recycler<DefaultChannelOutboundBuffer> RECYCLER =
            new Recycler<DefaultChannelOutboundBuffer>() {
        @Override
        protected DefaultChannelOutboundBuffer newObject(Handle<DefaultChannelOutboundBuffer> handle) {
            return new DefaultChannelOutboundBuffer(handle);
        }
    };

    /**
     * Get a new instance of this {@link ChannelOutboundBuffer} and attach it the given {@link AbstractChannel}
     */
    static ChannelOutboundBuffer newInstance(AbstractChannel channel) {
        ChannelOutboundBuffer buffer = RECYCLER.get();
        buffer.channel = channel;
        return buffer;
    }

    private final Recycler.Handle<? extends ChannelOutboundBuffer> handle;

    // A circular buffer used to store messages.  The buffer is arranged such that:  flushed <= unflushed <= tail.  The
    // flushed messages are stored in the range [flushed, unflushed).  Unflushed messages are stored in the range
    // [unflushed, tail).
    private Entry[] buffer;
    private int flushed;
    private int unflushed;
    private int tail;

    protected DefaultChannelOutboundBuffer(Recycler.Handle<? extends ChannelOutboundBuffer> handle) {
        this.handle = handle;

        buffer = new Entry[INITIAL_CAPACITY];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = newEntry();
        }
    }

    /**
     * Return the array of {@link Entry}'s which hold the pending write requests in an circular array.
     */
    protected final Entry[] entries() {
        return buffer;
    }

    @Override
    protected void addMessage0(Object msg, int estimatedSize, ChannelPromise promise) {
        Entry e = buffer[tail++];
        e.msg = msg;
        e.pendingSize = estimatedSize;
        e.promise = promise;
        e.total = total(msg);

        tail &= buffer.length - 1;

        if (tail == flushed) {
            addCapacity();
        }
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
            e[i] = newEntry();
        }

        buffer = e;
        flushed = 0;
        unflushed = s;
        tail = n;
    }

    @Override
    public final void addFlush() {
        unflushed = tail;

        final int mask = buffer.length - 1;
        int i = flushed;
        while (i != unflushed && buffer[i].msg != null) {
            Entry entry = buffer[i];
            if (!entry.promise.setUncancellable()) {
                // Was cancelled so make sure we free up memory and notify about the freed bytes
                int pending = entry.cancel();
                decrementPendingOutboundBytes(pending);
            }
            i = i + 1 & mask;
        }
    }

    @Override
    public final Object current() {
        if (isEmpty()) {
            return null;
        } else {
            Entry entry = buffer[flushed];
            return entry.msg;
        }
    }

    @Override
    public final void progress(long amount) {
        Entry e = buffer[flushed];
        ChannelPromise p = e.promise;
        if (p instanceof ChannelProgressivePromise) {
            long progress = e.progress + amount;
            e.progress = progress;
            ((ChannelProgressivePromise) p).tryProgress(progress, e.total);
        }
    }

    @Override
    protected final int remove0() {

        Entry e = buffer[flushed];
        Object msg = e.msg;
        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        e.clear();

        flushed = flushed + 1 & buffer.length - 1;

        if (!e.cancelled) {
            // only release message, notify and decrement if it was not canceled before.
            safeRelease(msg);
            safeSuccess(promise);
            return size;
        }

        return 0;
    }

    @Override
    protected int remove0(Throwable cause) {
        Entry e = buffer[flushed];
        Object msg = e.msg;
        ChannelPromise promise = e.promise;
        int size = e.pendingSize;

        e.clear();

        flushed = flushed + 1 & buffer.length - 1;

        if (!e.cancelled) {
            // only release message, fail and decrement if it was not canceled before.
            safeRelease(msg);

            safeFail(promise, cause);
            return size;
        }

        return 0;
    }

    @Override
    public final int size() {
        return unflushed - flushed & buffer.length - 1;
    }

    @Override
    public final boolean isEmpty() {
        return unflushed == flushed;
    }

    @Override
    protected int failUnflushed(ClosedChannelException cause) {
        // Release all unflushed messages.
        final int unflushedCount = tail - unflushed & buffer.length - 1;
        try {
            int size = 0;
            for (int i = 0; i < unflushedCount; i++) {
                Entry e = buffer[unflushed + i & buffer.length - 1];

                size += e.pendingSize;

                e.pendingSize = 0;
                if (!e.cancelled) {
                    safeRelease(e.msg);
                    safeFail(e.promise, cause);
                }
                e.msg = null;
                e.promise = null;
            }
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

        RECYCLER.recycle(this, (Recycler.Handle<DefaultChannelOutboundBuffer>) handle);
    }

    /**
     * Create a new {@link Entry} to use for the internal datastructure. Sub-classes may override this use a special
     * sub-class.
     */
    protected Entry newEntry() {
        return new Entry();
    }

    /**
     * Return the index of the first flushed message.
     */
    protected final int flushed() {
        return flushed;
    }

    /**
     * Return the index of the first unflushed messages.
     */
    protected final int unflushed() {
        return unflushed;
    }

    protected static class Entry {
        Object msg;
        ChannelPromise promise;
        long progress;
        long total;
        int pendingSize;
        int count = -1;
        boolean cancelled;

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
            msg = null;
            promise = null;
            progress = 0;
            total = 0;
            pendingSize = 0;
            count = -1;
            cancelled = false;
        }
    }
}
