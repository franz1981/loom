/*
 * Copyright (c) 2026, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package java.lang;

import java.lang.MpscVirtualThreadScheduler.MpscTask;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import jdk.internal.misc.BlockingCarrier;

/**
 * The threads that run Blocker sections moved off the carriers of one
 * MpscVirtualThreadScheduler.
 *
 * <p>Hand-off: a virtual thread that begins a section on a carrier yields; the
 * scheduler's onContinue, on that carrier after the thread unmounted, passes its
 * continuation to {@link #execute}, and a worker runs it, so the thread makes the whole
 * section mounted on the worker. At the section's end the thread yields again
 * ({@link Worker#endBlocking}); onContinue, on the worker, makes the worker IDLE, then
 * submits the thread to its home carrier.
 *
 * <p>Each worker has a slot, an idle bit and a hand-off word, {@code hand}:
 * <pre>
 *   BUSY -> IDLE     the worker, once its thread unmounted: in onContinue for the yield
 *                    at the section's end, else when the continuation returns; then it
 *                    sets its idle bit
 *   IDLE -> task     a claimer (a carrier in execute, or the start token's holder): CAS,
 *                    then unpark
 *   task -> BUSY     the worker, after a park returns: takes the task, clears its bit
 *   IDLE -> EXITED   the worker, when the keep-alive passed: CAS, then clears its bit
 *                    and its slot and leaves the count
 *   (new)   BUSY     the start token's holder: a slot below cap, then start() with the
 *                    first task
 * </pre>
 * execute claims the idle worker in the lowest slot. If none is idle and fewer than cap
 * workers run, it pushes the task on {@code pending} and takes the start token if it is
 * free. The holder places every pending task on an idle worker; for those left it starts
 * one worker, which takes the first, pushes the rest back and holds the token next; or,
 * when no worker can start, resubmits them home. It releases the token by storing 0,
 * then reading pending, and takes it again if a task arrived.
 *
 * <p>Invariants:
 * <ul>
 * <li>{@code hand} is IDLE exactly when the worker can be claimed. Claim and exit are
 *     both CASes from IDLE, so a worker is claimed or exits, never both. The claimer
 *     stores the task before the unpark, and the worker reads {@code hand} after every
 *     park, so no hand-off is lost and none runs twice.
 * <li>An idle bit is a hint written only by its worker; {@code hand} is the truth.
 * <li>{@code count <= cap}, and {@code count} is at least the occupied slots (an exit
 *     clears its slot before it leaves the count), so a reservation finds a free slot.
 *     Only the token's holder fills slots.
 * <li>A task given to execute ends in an IDLE worker's hand, in a false return (the
 *     caller resubmits it), or in pending. A pusher pushes, then reads the token; a
 *     holder stores 0, then reads pending: every pending task reaches a holder, which
 *     gives it to a worker or resubmits it home.
 * <li>At most one start is in flight: a carrier starts at most one thread per token it
 *     takes, and new workers start the rest of a burst, one after the other.
 * <li>A section's end makes its worker IDLE before the thread can begin its next
 *     section, so N threads never need more than N workers.
 * <li>Claims go to the lowest slot first, so workers above the concurrent load stay
 *     unclaimed and exit after the keep-alive.
 * <li>A worker runs a thread's continuation only after the thread unmounted from its
 *     carrier: execute is called only from onContinue for that thread's yield.
 * </ul>
 * A moved thread that parks or blocks on a monitor on its worker is woken onto its
 * scheduler and ends its section on a carrier; one whose yield at the end fails (pinned)
 * keeps the worker until it next unmounts. Either way the worker becomes IDLE when the
 * continuation returns.
 *
 * <p>A failed start sends its tasks home, where their sections run on carriers. After the
 * n-th failed start in a row (n at most 20), the next 2^n - 1 sections that would start a
 * worker stay on their carriers without trying; a successful start resets n. So a
 * recovery is delayed by at most as many sections as the failures already kept on
 * carriers.
 *
 * <p>Workers are named mpsc-helper-N. An idle worker exits after
 * {@code jdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis} (default 30000).
 */
final class MpscOffloadPool {
    private static final long KEEP_ALIVE_NANOS = TimeUnit.MILLISECONDS.toNanos(Math.max(1L,
            Long.getLong("jdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis", 30_000L)));

    // ForkJoinPool's MAX_CAP
    private static final int MAX_CAP = 0x7fff;
    private static final int MAX_FAILED_STARTS = 20;

    private static final Object IDLE = new Object();
    private static final Object BUSY = new Object();
    private static final Object EXITED = new Object();

    private static final VarHandle COUNT;
    private static final VarHandle PENDING;
    private static final VarHandle START_TOKEN;
    private static final VarHandle HAND;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            COUNT = l.findVarHandle(MpscOffloadPool.class, "count", int.class);
            PENDING = l.findVarHandle(MpscOffloadPool.class, "pending", MpscTask.class);
            START_TOKEN = l.findVarHandle(MpscOffloadPool.class, "startToken", int.class);
            HAND = l.findVarHandle(Worker.class, "hand", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final ThreadGroup group;
    private final int cap;
    // filled by the token's holder before the start, cleared by the exiting worker;
    // a claimer reads a slot after its idle bit, which the worker sets once it runs
    private final Worker[] slots;
    private final IdleSet idle;

    // the workers started or starting that have not exited
    private volatile int count;
    // a stack linked through MpscTask.nextPending
    private volatile MpscTask pending;
    private volatile int startToken;

    // the token's holder only
    private int nextId;
    private int failedStarts;   // in a row, at most MAX_FAILED_STARTS
    private int startsToSkip;

    MpscOffloadPool(ThreadGroup group, int cap) {
        this.group = group;
        this.cap = Math.min(cap, MAX_CAP);
        this.slots = new Worker[this.cap];
        this.idle = new IdleSet(this.cap);
    }

    /**
     * Hands a moving thread's continuation to an idle worker, or queues it for a new one
     * while fewer than cap run. Called on the carrier the thread left, after it unmounted.
     * @return false if every worker is busy and cap run: the caller resubmits it
     */
    boolean execute(MpscTask task) {
        if (claimIdle(task)) {
            placePending();
            return true;
        }
        if (count >= cap) {
            placePending();
            return false;
        }
        push(task, task);
        if (startToken == 0 && START_TOKEN.compareAndSet(this, 0, 1)) {
            place();
        }
        return true;
    }

    /**
     * Whether execute could take a thread now: one is idle, or fewer than cap run. A
     * stale answer is benign: the thread runs its section on a carrier, as at the cap.
     */
    boolean mayTake() {
        return idle.anyIdle() || count < cap;
    }

    /** The workers started that have not exited. */
    int poolSize() {
        return count;
    }

    /** The workers that are not IDLE; each carries at most one virtual thread. */
    int busyCount() {
        return Math.max(0, count - idle.count());
    }

    private boolean claimIdle(MpscTask task) {
        IdleSet idle = this.idle;
        for (int w = 0, n = idle.wordCount(); w < n; w++) {
            for (long bits = idle.word(w); bits != 0; bits &= bits - 1) {
                Worker k = slots[(w << 6) + Long.numberOfTrailingZeros(bits)];
                if (k != null && k.hand == IDLE && HAND.compareAndSet(k, IDLE, task)) {
                    LockSupport.unpark(k);
                    return true;
                }
            }
        }
        return false;
    }

    private void push(MpscTask first, MpscTask last) {
        for (;;) {
            MpscTask head = pending;
            last.nextPending = head;
            if (PENDING.compareAndSet(this, head, first)) {
                return;
            }
        }
    }

    private void pushAll(MpscTask chain) {
        MpscTask last = chain;
        while (last.nextPending != null) {
            last = last.nextPending;
        }
        push(chain, last);
    }

    /**
     * Places tasks left pending by a throw in place(), which released the token without a
     * rescan: called where a section's hand-off or end happens anyway.
     */
    private void placePending() {
        if (pending != null && startToken == 0 && START_TOKEN.compareAndSet(this, 0, 1)) {
            place();
        }
    }

    /**
     * With the start token: places the pending tasks on idle workers, then starts a
     * worker with those left, which holds the token next, or resubmits them home; then
     * releases the token, and takes it again if a task was pushed meanwhile. A throw
     * (a resubmit's OutOfMemoryError) leaves the tasks not yet resubmitted pending,
     * releases the token, and is reported: the next hand-off, cap refusal or section end
     * places them.
     */
    private void place() {
        try {
            do {
                MpscTask left = null;
                for (MpscTask t = (MpscTask) PENDING.getAndSet(this, (MpscTask) null), next;
                        t != null; t = next) {
                    next = t.nextPending;
                    t.nextPending = null;
                    if (!claimIdle(t)) {
                        t.nextPending = left;
                        left = t;
                    }
                }
                if (left != null && start(left)) {
                    return;
                }
                startToken = 0;
            } while (pending != null && START_TOKEN.compareAndSet(this, 0, 1));
        } catch (Throwable e) {
            startToken = 0;
            try {
                Thread.currentThread().dispatchUncaughtException(e);
            } catch (Throwable ignore) {
                // the handler's own error is dropped
            }
        }
    }

    /**
     * Starts a worker with the chain; else, at the cap, while starts are skipped, or if
     * the start fails, resubmits the chain home.
     * @return true if a worker started, and the token passed to it
     */
    private boolean start(MpscTask chain) {
        int slot = -1;
        if (startsToSkip == 0) {
            slot = reserve();
        }
        if (slot >= 0) {
            // reset before the start: the new worker holds the token, and its starts, next
            int failed = failedStarts;
            failedStarts = 0;
            try {
                Worker w = new Worker(this, slot, nextId++, chain);
                slots[slot] = w;
                w.start();
                return true;
            } catch (Throwable e) {
                slots[slot] = null;
                COUNT.getAndAdd(this, -1);
                failedStarts = Math.min(failed + 1, MAX_FAILED_STARTS);
                startsToSkip = (1 << failedStarts) - 1;
                resubmitHome(chain);
                return false;
            }
        }
        int sent = resubmitHome(chain);
        startsToSkip = Math.max(0, startsToSkip - sent);
        return false;
    }

    /** A free slot, counted, or -1 at the cap. */
    private int reserve() {
        for (int c; (c = count) < cap; ) {
            if (COUNT.compareAndSet(this, c, c + 1)) {
                for (int i = 0; i < slots.length; i++) {
                    if (slots[i] == null) {
                        return i;
                    }
                }
                COUNT.getAndAdd(this, -1);
                return -1;
            }
        }
        return -1;
    }

    /** Submits each task of the chain to its home; on a throw the rest go back on pending. */
    private int resubmitHome(MpscTask chain) {
        int sent = 0;
        for (MpscTask t = chain, next; t != null; t = next, sent++) {
            next = t.nextPending;
            t.nextPending = null;
            try {
                MpscVirtualThreadScheduler.submitHome(t);
            } catch (Throwable e) {
                t.nextPending = next;
                pushAll(t);
                throw e;
            }
        }
        return sent;
    }

    /**
     * After its task: makes w IDLE unless the end of a section did, places any tasks
     * left pending, then waits for a task. Returns it, or null if the keep-alive passed
     * and w exited.
     */
    private MpscTask await(Worker w) {
        w.release();
        placePending();
        long deadline = System.nanoTime() + KEEP_ALIVE_NANOS;
        for (;;) {
            if (w.hand instanceof MpscTask t) {
                w.take();
                return t;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                if (HAND.compareAndSet(w, IDLE, EXITED)) {
                    leave(w);
                    return null;
                }
                continue;
            }
            // a set interrupt status would make every park return at once
            Thread.interrupted();
            LockSupport.parkNanos(this, remaining);
        }
    }

    /** w exits: its bit and slot are cleared before it leaves the count. */
    private void leave(Worker w) {
        idle.clearIdle(w.slot);
        slots[w.slot] = null;
        COUNT.getAndAdd(this, -1);
    }

    /**
     * A pool thread, the carrier of the threads it runs: a section begun here runs here,
     * and the end of one moves the thread home.
     */
    static final class Worker extends Thread implements BlockingCarrier {
        private final MpscOffloadPool pool;
        private final int slot;
        private volatile Object hand = BUSY;
        // the tasks it starts with, linked through nextPending; read once by run
        private MpscTask first;
        // IDLE since its task's thread unmounted; own thread only
        private boolean released;

        Worker(MpscOffloadPool pool, int slot, int id, MpscTask first) {
            super(pool.group, null, "mpsc-helper-" + id, 0, false);
            this.pool = pool;
            this.slot = slot;
            this.first = first;
            setDaemon(true);
        }

        /**
         * Takes the first task, pushes the rest back and places them, then runs tasks
         * until the keep-alive passes. If it ends by a throw, it leaves the pool, and a
         * task it has not run goes home.
         */
        @Override
        public void run() {
            MpscTask task = first;
            first = null;
            boolean exited = false;
            try {
                MpscTask rest = task.nextPending;
                if (rest != null) {
                    task.nextPending = null;
                    pool.pushAll(rest);
                }
                pool.place();
                while (task != null) {
                    MpscTask t = task;
                    task = null;
                    runTask(t);
                    task = pool.await(this);
                }
                exited = true;
            } finally {
                if (!exited) {
                    Object h = HAND.getAndSet(this, EXITED);
                    if (h instanceof MpscTask t) {
                        task = t;
                    }
                    pool.leave(this);
                    if (task != null) {
                        MpscVirtualThreadScheduler.submitHome(task);
                    }
                }
            }
        }

        private void runTask(MpscTask task) {
            try {
                task.run();
            } catch (Throwable e) {
                try {
                    dispatchUncaughtException(e);
                } catch (Throwable ignore) {
                    // the handler's own error is dropped
                }
            }
        }

        /**
         * Makes this worker IDLE, once per task. Invoked by the scheduler's onContinue, on
         * this worker, for the yield at a section's end, before the thread is submitted
         * home, and by the worker after its task. A second call is a no-op: onContinue is
         * called again for the same yield after an OutOfMemoryError, and the hand may hold
         * a task claimed meanwhile.
         */
        void release() {
            if (released) {
                return;
            }
            released = true;
            hand = IDLE;
            pool.idle.setIdle(slot);
        }

        private void take() {
            hand = BUSY;
            pool.idle.clearIdle(slot);
            released = false;
        }

        /** A section that begins on a pool thread runs on it. */
        @Override
        public boolean beginBlocking() {
            return false;
        }

        /**
         * Yields the mounted virtual thread at the end of its section, so that it moves
         * home. If the yield fails (pinned) or throws, the thread goes on here until it
         * next unmounts.
         */
        @Override
        public void endBlocking() {
            try {
                Thread.yield();
            } catch (Throwable e) { }
        }
    }
}
