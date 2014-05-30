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
package io.netty.channel;

import java.util.Arrays;

/**
 * This implementation allows to register {@link ChannelFuture} instances which will get notified once some amount of
 * data was written and so a checkpoint was reached.
 */
public final class ChannelFlushPromiseNotifier {

    private long writeCounter;
    private FlushCheckpoint[] flushCheckpoints;
    private int head;
    private int tail;
    private final boolean tryNotify;
    private final int initialCapacity;

    /**
     * Create a new instance
     *
     * @param tryNotify if {@code true} the {@link ChannelPromise}s will get notified with
     *                  {@link ChannelPromise#trySuccess()} and {@link ChannelPromise#tryFailure(Throwable)}.
     *                  Otherwise {@link ChannelPromise#setSuccess()} and {@link ChannelPromise#setFailure(Throwable)}
     *                  is used
     */
    public ChannelFlushPromiseNotifier(boolean tryNotify) {
        this(tryNotify, 16);
    }

    public ChannelFlushPromiseNotifier(boolean tryNotify, int initialCapacity) {
        this.tryNotify = tryNotify;
        flushCheckpoints = new FlushCheckpoint[powerOfTwo(initialCapacity)];
        this.initialCapacity = flushCheckpoints.length;
    }

    /**
     * Create a new instance which will use {@link ChannelPromise#setSuccess()} and
     * {@link ChannelPromise#setFailure(Throwable)} to notify the {@link ChannelPromise}s.
     */
    public ChannelFlushPromiseNotifier() {
        this(false);
    }

    /**
     * Add a {@link ChannelPromise} to this {@link ChannelFlushPromiseNotifier} which will be notified after the given
     * pendingDataSize was reached.
     */
    public ChannelFlushPromiseNotifier add(ChannelPromise promise, int pendingDataSize) {
        if (promise == null) {
            throw new NullPointerException("promise");
        }
        if (pendingDataSize < 0) {
            throw new IllegalArgumentException("pendingDataSize must be >= 0 but was" + pendingDataSize);
        }
        if (promise instanceof VoidChannelPromise) {
            return this;
        }
        long checkpoint = writeCounter + pendingDataSize;
        FlushCheckpoint cp;
        if (promise instanceof FlushCheckpoint) {
            cp = (FlushCheckpoint) promise;
            cp.flushCheckpoint(checkpoint);
            cp.pendingDataSize(pendingDataSize);
        } else {
            cp = new DefaultFlushCheckpoint(checkpoint, pendingDataSize, promise);
        }
        flushCheckpoints[tail] = cp;
        tail = nextIdx(tail);
        if (tail == head) {
            int s = flushCheckpoints.length;
            int newCapacity = s << 1;
            if (newCapacity < 0) {
                throw new IllegalStateException();
            }
            FlushCheckpoint[] checkpoints = new FlushCheckpoint[newCapacity];
            System.arraycopy(flushCheckpoints, 0, checkpoints, 0, s);
            flushCheckpoints = checkpoints;
            head = 0;
            tail = s;
        }
        return this;
    }

    /**
     * Increase the current write counter by the given delta
     */
    public ChannelFlushPromiseNotifier increaseWriteCounter(long delta) {
        if (delta < 0) {
            throw new IllegalArgumentException("delta must be >= 0 but was" + delta);
        }
        writeCounter += delta;
        return this;
    }

    /**
     * Return the current write counter of this {@link ChannelFlushPromiseNotifier}
     */
    public long writeCounter() {
        return writeCounter;
    }

    /**
     * Notify all {@link ChannelFuture}s that were registered with {@link #add(ChannelPromise, int)} and
     * their pendingDatasize is smaller after the the current writeCounter returned by {@link #writeCounter()}.
     *
     * After a {@link ChannelFuture} was notified it will be removed from this {@link ChannelFlushPromiseNotifier} and
     * so not receive anymore notification.
     */
    public ChannelFlushPromiseNotifier notifyFlushFutures() {
        notifyFlushFutures0(null);
        return this;
    }

    /**
     * Notify all {@link ChannelFuture}s that were registered with {@link #add(ChannelPromise, int)} and
     * their pendingDatasize isis smaller then the current writeCounter returned by {@link #writeCounter()}.
     *
     * After a {@link ChannelFuture} was notified it will be removed from this {@link ChannelFlushPromiseNotifier} and
     * so not receive anymore notification.
     *
     * The rest of the remaining {@link ChannelFuture}s will be failed with the given {@link Throwable}.
     *
     * So after this operation this {@link ChannelFutureListener} is empty.
     */
    public ChannelFlushPromiseNotifier notifyFlushFutures(Throwable cause) {
        notifyFlushFutures();
        for (;;) {
            FlushCheckpoint cp = pollCheckpoint();
            if (cp == null) {
                break;
            }
            if (tryNotify) {
                cp.promise().tryFailure(cause);
            } else {
                cp.promise().setFailure(cause);
            }
        }
        return this;
    }

    private FlushCheckpoint pollCheckpoint()  {
        FlushCheckpoint cp = flushCheckpoints[head];
        if (cp != null) {
            removeHead();
        }
        return cp;
    }

    private void removeHead() {
        assert !isEmpty();
        // null out (to allow GC) and update head index
        flushCheckpoints[head] = null;
        head = nextIdx(head);
    }

    private int nextIdx(int index) {
        // use bitwise operation as this is faster as using modulo.
        return (index + 1) & flushCheckpoints.length - 1;
    }

    public int size()  {
        return tail - head & flushCheckpoints.length - 1;
    }

    private static int powerOfTwo(int res) {
        if (res <= 2) {
            return 2;
        }
        res--;
        res |= res >> 1;
        res |= res >> 2;
        res |= res >> 4;
        res |= res >> 8;
        res |= res >> 16;
        res++;
        return res;
    }

    /**
     * Notify all {@link ChannelFuture}s that were registered with {@link #add(ChannelPromise, int)} and
     * their pendingDatasize is smaller then the current writeCounter returned by {@link #writeCounter()} using
     * the given cause1.
     *
     * After a {@link ChannelFuture} was notified it will be removed from this {@link ChannelFlushPromiseNotifier} and
     * so not receive anymore notification.
     *
     * The rest of the remaining {@link ChannelFuture}s will be failed with the given {@link Throwable}.
     *
     * So after this operation this {@link ChannelFutureListener} is empty.
     *
     * @param cause1    the {@link Throwable} which will be used to fail all of the {@link ChannelFuture}s whichs
     *                  pendingDataSize is smaller then the current writeCounter returned by {@link #writeCounter()}
     * @param cause2    the {@link Throwable} which will be used to fail the remaining {@link ChannelFuture}s
     */
    public ChannelFlushPromiseNotifier notifyFlushFutures(Throwable cause1, Throwable cause2) {
        notifyFlushFutures0(cause1);
        for (;;) {
            FlushCheckpoint cp = pollCheckpoint();
            if (cp == null) {
                break;
            }
            if (tryNotify) {
                cp.promise().tryFailure(cause2);
            } else {
                cp.promise().setFailure(cause2);
            }
        }
        return this;
    }

    private void notifyFlushFutures0(Throwable cause) {
        int size = size();
        if (size == 0) {
            writeCounter = 0;
            return;
        }

        final long writeCounter = this.writeCounter;
        for (;;) {
            FlushCheckpoint cp = flushCheckpoints[head];
            if (cp == null) {
                // Reset the counter if there's nothing in the notification list.
                this.writeCounter = 0;
                break;
            }

            if (cp.flushCheckpoint() > writeCounter) {
                long delta = cp.flushCheckpoint() - writeCounter;
                ChannelPromise p = cp.promise();
                if (p instanceof ChannelProgressivePromise) {
                    ((ChannelProgressivePromise) p).tryProgress(delta, cp.pendingDataSize());
                }

                if (writeCounter > 0 && size == 1) {
                    this.writeCounter = 0;
                    cp.flushCheckpoint(delta);
                }
                break;
            }

            removeHead();

            if (cause == null) {
                if (tryNotify) {
                    cp.promise().trySuccess();
                } else {
                    cp.promise().setSuccess();
                }
            } else {
                if (tryNotify) {
                    cp.promise().tryFailure(cause);
                } else {
                    cp.promise().setFailure(cause);
                }
            }
            size--;
        }

        // Avoid overflow
        final long newWriteCounter = this.writeCounter;
        if (newWriteCounter >= 0x8000000000L) {
            // Reset the counter only when the counter grew pretty large
            // so that we can reduce the cost of updating all entries in the notification list.
            this.writeCounter = 0;
            for (FlushCheckpoint cp: flushCheckpoints) {
                cp.flushCheckpoint(cp.flushCheckpoint() - newWriteCounter);
            }
        }
    }

    /**
     * Empty this {@link ChannelFlushPromiseNotifier} and reset it to its {@code initialCapacity}
     */
    public void reset() {
        if (flushCheckpoints.length > initialCapacity) {
            flushCheckpoints = new FlushCheckpoint[initialCapacity];
        } else {
            Arrays.fill(flushCheckpoints, null);
        }
    }

    /**
     * Returns {@code true} if empty.
     */
    public boolean isEmpty() {
        return head == tail;
    }

    interface FlushCheckpoint {
        long pendingDataSize();
        void pendingDataSize(long pendingDataSize);
        long flushCheckpoint();
        void flushCheckpoint(long checkpoint);
        ChannelPromise promise();
    }

    private static class DefaultFlushCheckpoint implements FlushCheckpoint {
        private long checkpoint;
        private long pendingDataSize;
        private final ChannelPromise future;

        DefaultFlushCheckpoint(long checkpoint, long pendingDataSize, ChannelPromise future) {
            this.checkpoint = checkpoint;
            this.future = future;
            this.pendingDataSize = pendingDataSize;
        }

        @Override
        public long pendingDataSize() {
            return pendingDataSize;
        }

        @Override
        public void pendingDataSize(long pendingDataSize) {
            this.pendingDataSize = pendingDataSize;
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
