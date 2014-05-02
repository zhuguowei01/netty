package org.jboss.netty.util;
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

import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.internal.DetectionUtil;
import org.jboss.netty.util.internal.SharedResourceMisuseDetector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

public class LightHashedWheelTimer implements Timer {

    private static final SharedResourceMisuseDetector misuseDetector =
            new SharedResourceMisuseDetector(HashedWheelTimer.class);

    private static final InternalLogger logger =
            InternalLoggerFactory.getInstance(LightHashedWheelTimer.class);
    private static final AtomicIntegerFieldUpdater<LightHashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(LightHashedWheelTimer.class, "workerState");
    private static final AtomicLongFieldUpdater<LightHashedWheelTimer> TICK_UPDATER =
            AtomicLongFieldUpdater.newUpdater(LightHashedWheelTimer.class, "tick");

    private final Worker worker = new Worker();
    private final Thread workerThread;

    public static final int WORKER_STATE_INIT = 0;
    public static final int WORKER_STATE_STARTED = 1;
    public static final int WORKER_STATE_SHUTDOWN = 2;
    @SuppressWarnings({ "unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
    private volatile int workerState = WORKER_STATE_INIT; // 0 - init, 1 - started, 2 - shut down

    private final long tickDuration;
    private final HashedWheelBucket[] wheel;
    private final int mask;
    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);
    private volatile long startTime;

    @SuppressWarnings("unused")
    private volatile long tick;

    /**
     * Creates a new timer with the default thread factory
     * ({@link java.util.concurrent.Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public LightHashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is <= 0
     */
    public LightHashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @param ticksPerWheel  the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is <= 0
     */
    public LightHashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory  a {@link java.util.concurrent.ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public LightHashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory  a {@link ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is <= 0
     */
    public LightHashedWheelTimer(
            ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory  a {@link ThreadFactory} that creates a
     *                       background {@link Thread} which is dedicated to
     *                       {@link TimerTask} execution.
     * @param tickDuration   the duration between tick
     * @param unit           the time unit of the {@code tickDuration}
     * @param ticksPerWheel  the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is <= 0
     */
    public LightHashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        workerThread = threadFactory.newThread(worker);

        // Normalize ticksPerWheel to power of two and initialize the wheel.
        wheel = createWheel(ticksPerWheel, workerThread);
        mask = wheel.length - 1;

        // Convert tickDuration to nanos.
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow.
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
                    tickDuration, Long.MAX_VALUE / wheel.length));
        }

        // Misuse check
        misuseDetector.increase();
    }

    @SuppressWarnings("unchecked")
    private static HashedWheelBucket[] createWheel(int ticksPerWheel, Thread workerThread) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException(
                    "ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i ++) {
            wheel[i] = new HashedWheelBucket(workerThread);
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = 1;
        while (normalizedTicksPerWheel < ticksPerWheel) {
            normalizedTicksPerWheel <<= 1;
        }
        return normalizedTicksPerWheel;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        while (startTime == 0) {
            try {
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    public Set<Timeout> stop() {
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            WORKER_STATE_UPDATER.set(this, WORKER_STATE_SHUTDOWN);

            return Collections.emptySet();
        }

        boolean interrupted = false;
        while (workerThread.isAlive()) {
            workerThread.interrupt();
            try {
                workerThread.join(100);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        misuseDetector.decrease();

        return worker.notProcessedTimeouts();
    }

    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        start();

        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Add the timeout to the wheel.
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        wheel[timeout.stopIndex].add(timeout);

        return timeout;
    }

    private final class Worker implements Runnable {
        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();

        Worker() {
        }

        public void run() {
            // Initialize the startTime.
            startTime = System.nanoTime();
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            // Notify the other threads waiting for the initialization at start().
            startTimeInitialized.countDown();

            do {
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    HashedWheelBucket bucket =
                            wheel[(int) (TICK_UPDATER.getAndIncrement(LightHashedWheelTimer.this) & mask)];
                    bucket.expire(deadline);
                }
            } while (WORKER_STATE_UPDATER.get(LightHashedWheelTimer.this) == WORKER_STATE_STARTED);
            for (HashedWheelBucket bucket: wheel) {
                bucket.clear(unprocessedTimeouts);
            }
        }

        /**
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         */
        private long waitForNextTick() {
            long deadline = tickDuration * (TICK_UPDATER.get(LightHashedWheelTimer.this) + 1);

            for (;;) {
                final long currentTime = System.nanoTime() - startTime;
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                // Check if we run on windows, as if thats the case we will need
                // to round the sleepTime as workaround for a bug that only affect
                // the JVM if it runs on windows.
                //
                // See https://github.com/netty/netty/issues/356
                if (DetectionUtil.isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException e) {
                    if (WORKER_STATE_UPDATER.get(LightHashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        public Set<Timeout> notProcessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    @SuppressWarnings("serial")
    private static final class HashedWheelTimeout extends HashedWheelTimeoutNode {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

        private final LightHashedWheelTimer timer;
        private final TimerTask task;
        private final long deadline;
        private final int stopIndex;
        private long remainingRounds;
        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization" })
        private volatile int state = ST_INIT;

        HashedWheelTimeout(LightHashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;

            long t = TICK_UPDATER.get(timer);
            long calculated = deadline / timer.tickDuration;
            final long ticks = Math.max(calculated, t); // Ensure we don't schedule for past.
            stopIndex = (int) (ticks & timer.mask);
            remainingRounds = (calculated - t) / timer.wheel.length;
        }

        @Override
        public Timer getTimer() {
            return timer;
        }

        @Override
        public TimerTask getTask() {
            return task;
        }

        @Override
        public void cancel() {
            STATE_UPDATER.compareAndSet(this, ST_INIT, ST_CANCELLED);
        }

        @Override
        public boolean isCancelled() {
            return STATE_UPDATER.get(this) == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return STATE_UPDATER.get(this) != ST_INIT;
        }

        @Override
        public void expire() {
            if (!STATE_UPDATER.compareAndSet(this, ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        long deadline() {
            return deadline;
        }

        @Override
        long remainingRounds() {
            return remainingRounds;
        }

        @Override
        void decrementRemaingRounds() {
            remainingRounds--;
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;

            StringBuilder buf = new StringBuilder(192);
            buf.append(getClass().getSimpleName());
            buf.append('(');

            buf.append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining);
                buf.append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining);
                buf.append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            buf.append(", task: ");
            buf.append(getTask());

            return buf.append(')').toString();
        }
    }

    /**
     * Hashed-Wheel-Bucket for MPSC work-pattern.
     */
    @SuppressWarnings("serial")
    static final class HashedWheelBucket extends AtomicReference<HashedWheelTimeoutNode> {
        // just used for asserts
        private final Thread workerThread;
        @SuppressWarnings({ "unused", "FieldMayBeFinal" })
        private AtomicReference<HashedWheelTimeoutNode> tail;

        HashedWheelBucket(Thread workerThread) {
            this.workerThread = workerThread;
            HashedWheelTimeoutNode node = new HashedWheelTimeoutNode();
            tail = new AtomicReference<HashedWheelTimeoutNode>(node);
            set(node);
        }

        /**
         * Add {@link HashedWheelTimeoutNode} to this bucket.
         */
        public void add(HashedWheelTimeoutNode timeout) {
            timeout.setNext(null);
            getAndSet(timeout).setNext(timeout);
        }

        /**
         * Expire all {@link HashedWheelTimeoutNode}s for the given {@code deadline}.
         */
        public void expire(long deadline) {
            assert Thread.currentThread() == workerThread;
            HashedWheelTimeoutNode last = get();

            for (;;) {
                HashedWheelTimeoutNode node = pollNode();

                if (node == null) {
                    // all nodes are processed
                    break;
                }

                if (node.remainingRounds() <= 0) {
                    if (node.deadline() <= deadline) {
                        node.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new Error(String.format(
                                "timeout.deadline (%d) > deadline (%d)", node.deadline(), deadline));
                    }
                } else if (!node.isCancelled()) {
                    // decrement and add again
                    node.decrementRemaingRounds();
                    add(node);
                }
                if (node == last) {
                    // We reached the node that was the last node when we started so stop here.
                    break;
                }
            }
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         */
        public void clear(Set<Timeout> set) {
            assert Thread.currentThread() == workerThread;
            for (;;) {
                HashedWheelTimeoutNode node = pollNode();
                if (node == null) {
                    return;
                }
                if (node.isExpired() || node.isCancelled()) {
                    continue;
                }
                set.add(node);
            }
        }

        private HashedWheelTimeoutNode pollNode() {
            HashedWheelTimeoutNode next;
            for (;;) {
                final HashedWheelTimeoutNode tail = this.tail.get();
                next = tail.next();
                if (next != null || get() == tail) {
                    break;
                }
            }
            if (next == null) {
                return null;
            }
            final HashedWheelTimeoutNode ret = next;
            // lazySet would be the best but java5 has no lazySet :(
            tail.set(next);
            return ret;
        }
    }

    @SuppressWarnings("serial")
    static class HashedWheelTimeoutNode extends AtomicReference<HashedWheelTimeoutNode> implements Timeout {

        final HashedWheelTimeoutNode next() {
            return get();
        }

        final void setNext(HashedWheelTimeoutNode next) {
            lazySet(next);
        }

        public Timer getTimer() {
            throw new UnsupportedOperationException();
        }

        public TimerTask getTask() {
            throw new UnsupportedOperationException();
        }

        public boolean isExpired() {
            // never expires
            return false;
        }

        public boolean isCancelled() {
            // never is cancelled
            return false;
        }

        public void cancel() {
            // noop
        }

        public void expire() {
            // noop
        }

        long deadline() {
            return 0;
        }

        long remainingRounds() {
            return 1;
        }

        void decrementRemaingRounds() {
            // noop
        }
    }
}
