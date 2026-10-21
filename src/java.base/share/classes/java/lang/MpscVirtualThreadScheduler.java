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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.Thread.VirtualThreadScheduler;
import java.lang.Thread.VirtualThreadTask;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import jdk.internal.vm.annotation.Contended;
import sun.nio.ch.CarrierLocalPoller;

/**
 * An alternative virtual thread scheduler using a single MPSC queue per carrier.
 * No work stealing — each carrier drains only its own queue — unless
 * {@code jdk.virtualThreadScheduler.mpscSteal} is set; see {@link #STEAL}.
 *
 * <p>External submissions use a per-thread probe: each submitting platform
 * thread targets one carrier for its lifetime, and different threads are spread
 * across carriers. Carrier affinity is set once at start via affinityHint;
 * onContinue always routes back to the same carrier.
 *
 * <p>With poller Mode 4 (CARRIER_LOCAL_POLLER), each carrier owns its own
 * epoll fd. VT fds register directly — no sub-pollers, no master poller.
 * The carrier interleaves task draining with I/O polling.
 */
final class MpscVirtualThreadScheduler implements VirtualThreadScheduler {

    /**
     * Opt-in work stealing. Off by default, and when off none of the code below that
     * tests it runs: the queues, loops and wakeups are the ones without stealing.
     *
     * When on, each carrier has a second queue, {@code stealQueue}, for the tasks of
     * virtual threads that may run elsewhere; the tasks of explicitly placed threads
     * (affinityHint set before start, e.g. by roundRobinAffinity()) and of sticky
     * threads stay in {@code queue} and never leave their carrier. Only the
     * stealable queue is ever consumed by another carrier, so only it pays for a
     * second consumer (a spin lock around each poll).
     *
     * No carrier judges its own load. Help is triggered by facts only: stealable work
     * is queued on a carrier that is not parked, some carrier is parked, and nobody
     * is already searching. Whoever observes those facts takes the single searcher
     * token and wakes one parked carrier with the victim encoded in its state
     * (SEARCHING + victim id). The woken carrier steals one task from that victim,
     * hands the token to the next parked carrier if the victim still has stealable
     * work, otherwise releases it, and then runs the task. A carrier about to park
     * also looks at every running carrier's stealable queue and may take one task.
     *
     * Locality gate. A thief takes a task only if some task that was already queued
     * at its previous look at that victim is still queued now: the work has waited
     * across an interval of the thief's own activity (for a searcher, the interval is
     * its wake, and the previous look is the signaller's). This is the two-look rule
     * of Shenango's congestion detection with the clock taken out; it reads only the
     * victim's head and tail indices, says nothing about the owner's load, and lets an
     * owner that keeps up with its queue keep its tasks. A look that refuses records
     * the tail for the next one, and marks the victim so that its own "left behind"
     * signals stop until it sees its stealable queue empty, so refusals cannot turn
     * into a stream of wakes.
     *
     * The facts are observed at three places: a remote producer whose task lands at
     * the head of a running carrier's stealable queue (the queue went empty to
     * non-empty); a consumer -- owner or thief -- that takes a task and leaves more
     * behind; and a searcher releasing the token after a successful steal, which
     * rescans once because signals were refused while it held it. A carrier
     * enqueueing to itself sends no signal: the owner gets its chance first, and its
     * own take of an earlier task is what signals for the ones behind it.
     */
    static final boolean STEAL = Boolean.getBoolean("jdk.virtualThreadScheduler.mpscSteal");

    /** With STEAL: whether sticky virtual threads are stealable too (default: no). */
    static final boolean STEAL_STICKY =
            Boolean.getBoolean("jdk.virtualThreadScheduler.mpscStealSticky");

    /**
     * With STEAL: whether a stolen virtual thread moves to the thief for good (its
     * affinityHint is rewritten) instead of returning to its carrier on its next
     * continue (default: it returns).
     */
    static final boolean STEAL_RELOCATE =
            Boolean.getBoolean("jdk.virtualThreadScheduler.mpscStealRelocate");

    /**
     * With STEAL, diagnostic: whether a refused look marks the victim (default true).
     * Off, only the searcher token and the emptiness checks bound signalling, as in
     * the Netty scheduler's nSearching chain.
     */
    static final boolean STEAL_MARK = !"false".equals(
            System.getProperty("jdk.virtualThreadScheduler.mpscStealMark"));

    /** With STEAL: count steal events and print them at exit (stable key=value lines). */
    static final boolean STEAL_STATS =
            Boolean.getBoolean("jdk.virtualThreadScheduler.mpscStealStats");

    private static final VarHandle SEARCH_TOKEN;
    private static final VarHandle PARKED_COUNT;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            SEARCH_TOKEN = l.findVarHandle(MpscVirtualThreadScheduler.class, "searching", int.class);
            PARKED_COUNT = l.findVarHandle(MpscVirtualThreadScheduler.class, "parkedCount", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final CarrierThread[] carriers;

    // With STEAL: the searcher token, 1 while a carrier woken to search holds it.
    @Contended("steal")
    private volatile int searching;

    // With STEAL: carriers between their PARKED store and leaving PARKED. Only a
    // filter: signalWork skips the token and the state scan when it reads 0.
    @Contended("parked")
    private volatile int parkedCount;

    // With STEAL_STATS: events counted on threads that are not this scheduler's carriers.
    private final AtomicLongArray externalStats;

    MpscVirtualThreadScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        this.carriers = new CarrierThread[parallelism];
        for (int i = 0; i < parallelism; i++) {
            carriers[i] = new CarrierThread(i, this, parallelism);
        }
        this.externalStats = STEAL_STATS ? new AtomicLongArray(Stat.COUNT) : null;
        if (STEAL_STATS) {
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(this::printStats));
            } catch (IllegalStateException e) { }
        }
        for (int i = 0; i < parallelism; i++) {
            carriers[i].start();
        }
    }

    @Override
    public void onStart(VirtualThreadTask task) {
        VirtualThread vt = (VirtualThread) task.thread();
        CarrierThread target;
        if (vt.affinityHint >= 0) {
            target = carriers[Math.floorMod(vt.affinityHint, carriers.length)];
        } else {
            target = carrierFor();
        }
        if (STEAL) {
            boolean placed = vt.affinityHint >= 0;
            vt.mpscStealable = !placed && (STEAL_STICKY || !vt.hasStickyAffinity());
        }
        vt.affinityHint = target.id;
        if (STEAL && vt.mpscStealable) {
            enqueueStealable(target, task);
        } else {
            enqueue(target, task);
        }
    }

    @Override
    public void onContinue(VirtualThreadTask task) {
        VirtualThread vt = (VirtualThread) task.thread();
        int hint = vt.affinityHint;
        if (hint >= 0 && hint < carriers.length) {
            if (STEAL && vt.mpscStealable) {
                enqueueStealable(carriers[hint], task);
            } else {
                enqueue(carriers[hint], task);
            }
            return;
        }
        onStart(task);
    }

    private static void enqueue(CarrierThread carrier, VirtualThreadTask task) {
        carrier.queue.offer(task);
        if (carrier.carrierState == CarrierThread.PARKED) {
            // A virtual thread never runs while its carrier's state reads PARKED: the
            // carrier stores RUNNING before it executes anything. So an enqueue that
            // observes PARKED from the carrier's own thread can only originate inside the
            // carrier's blocking poll -- in Mode 4, a readiness unpark from
            // CarrierLocalPoller.poll(-1) -- and the carrier unconditionally stores
            // RUNNING and drains the queue when that poll returns. Signalling it would
            // cost an eventfd write, and a later spurious wakeup, for a carrier already
            // on its way back to the drain loop.
            if (Thread.currentCarrierThread() == carrier) {
                return;
            }
            wake(carrier);
        }
    }

    private static void wake(CarrierThread carrier) {
        if (carrier.poller != null) {
            try {
                carrier.poller.wakeup();
            } catch (IOException e) {
                LockSupport.unpark(carrier);
            }
        } else {
            LockSupport.unpark(carrier);
        }
    }

    // ---- Work stealing (STEAL only) ----

    /**
     * Enqueues to the carrier's stealable queue. A parked carrier is woken exactly as
     * by enqueue(). A carrier that is not parked gets a searcher signalled for it when
     * this task is at the head of its stealable queue, i.e. the queue went from empty
     * to non-empty; a task behind others is covered by the consumer of the task ahead
     * of it (see pollStealable) -- the head test and that consumer's test are a Dekker
     * pair, so one of the two always signals.
     */
    private void enqueueStealable(CarrierThread carrier, VirtualThreadTask task) {
        var q = carrier.stealQueue;
        long index = q.offer(task);
        int state = carrier.carrierState;
        if (state == CarrierThread.PARKED) {
            // same reasoning as enqueue()
            if (Thread.currentCarrierThread() != carrier) {
                wake(carrier);
            }
        } else if (Thread.currentCarrierThread() == carrier) {
            // Local first: the carrier enqueued for itself (a child it started, a
            // continuation it triggered, or -- in SEARCHING -- a readiness unpark
            // inside its own poll). No signal; its own take of the task ahead of this
            // one, or of this one, is the leftBehind() that signals for the rest.
        } else if (q.consumerIndexVolatile() == index) {
            count(Stat.SIGNAL_AT_HEAD);
            signalWork(carrier, true);
        }
    }

    /**
     * Tries to wake one parked carrier to steal from the victim. Gives up when no
     * carrier is parked (a carrier that parks later scans all stealable queues after
     * publishing PARKED) or when a searcher is already out (it rescans on release).
     */
    void signalWork(CarrierThread victim, boolean rescan) {
        count(Stat.SIGNAL_ATTEMPT);
        if (parkedCount == 0) {
            count(Stat.SIGNAL_NO_PARKED_PRE);
            return;
        }
        if (searching != 0 || !SEARCH_TOKEN.compareAndSet(this, 0, 1)) {
            count(Stat.SIGNAL_TOKEN_BUSY);
            return;
        }
        count(Stat.TOKEN_ACQUIRED);
        if (wakeSearcher(victim)) {
            return;
        }
        count(Stat.SIGNAL_NO_PARKED_POST);
        releaseToken(rescan);
    }

    /**
     * With the token held, moves one parked carrier to SEARCHING + victim and wakes
     * it; the token passes to that carrier. Scans from the victim's neighbour so that
     * no carrier id is always preferred. Returns false, still holding the token, if no
     * carrier was parked.
     */
    boolean wakeSearcher(CarrierThread victim) {
        var carriers = this.carriers;
        int n = carriers.length;
        for (int i = 1; i < n; i++) {
            CarrierThread c = carriers[(victim.id + i) % n];
            if (c.carrierState != CarrierThread.PARKED) {
                continue;
            }
            // the signaller's look, read by c after the getAndSet in leaveParked; a
            // write to a carrier whose CAS then fails is never read
            c.searchTail = victim.stealQueue.tailIndex();
            if (STEAL_STATS) {
                c.searchSignalNanos = System.nanoTime();
            }
            if (CarrierThread.STATE.compareAndSet(c, CarrierThread.PARKED,
                                                         CarrierThread.SEARCHING + victim.id)) {
                count(Stat.SEARCHER_WOKEN);
                wake(c);
                return true;
            }
        }
        return false;
    }

    /**
     * Releases the token. With rescan, then looks once for a carrier that is not
     * parked and has stealable work, and signals for it without a further rescan:
     * signals refused while the token was held left no other trace.
     */
    void releaseToken(boolean rescan) {
        count(Stat.TOKEN_RELEASED);
        searching = 0;
        if (rescan) {
            CarrierThread v = findStealable(null);
            if (v != null) {
                count(Stat.SIGNAL_RESCAN);
                signalWork(v, false);
            }
        }
    }

    /**
     * Returns a carrier other than {@code self}, not parked, with stealable work, or
     * null. Starts after self so that no carrier id is always preferred.
     */
    CarrierThread findStealable(CarrierThread self) {
        var carriers = this.carriers;
        int n = carriers.length;
        int start = (self == null) ? 0 : self.id + 1;
        for (int i = 0; i < n; i++) {
            CarrierThread c = carriers[(start + i) % n];
            if (c != self
                    && c.carrierState == CarrierThread.RUNNING
                    && !c.stealQueue.isEmpty()) {
                return c;
            }
        }
        return null;
    }

    // ---- Stats (STEAL_STATS only) ----

    static final class Stat {
        static final int SIGNAL_AT_HEAD = 0;
        static final int SIGNAL_LEFT_BEHIND = 1;
        static final int SIGNAL_RESCAN = 2;
        static final int SIGNAL_ATTEMPT = 3;
        static final int SIGNAL_NO_PARKED_PRE = 4;
        static final int SIGNAL_TOKEN_BUSY = 5;
        static final int TOKEN_ACQUIRED = 6;
        static final int TOKEN_RELEASED = 7;
        static final int SEARCHER_WOKEN = 8;
        static final int SEARCH_FOUND = 9;
        static final int SEARCH_EMPTY = 10;
        static final int CHAIN = 11;
        static final int PREPARK_FOUND = 12;
        static final int STOLEN = 13;
        static final int RELOCATED = 14;
        static final int STEAL_LOCK_BUSY = 15;
        static final int SIGNAL_NO_PARKED_POST = 16;
        static final int SIGNAL_SUPPRESSED = 17;
        static final int SEARCH_REFUSED = 18;
        static final int SEARCH_ZERO_GAP = 19;
        static final int PREPARK_REFUSED = 20;
        static final int MARKED = 21;
        // searcher wake latency, signal to search: <1us, <10us, <100us, >=100us
        static final int WAKE_LT_1US = 22;
        static final int WAKE_LT_10US = 23;
        static final int WAKE_LT_100US = 24;
        static final int WAKE_GE_100US = 25;
        static final int COUNT = 26;
        static final String[] NAMES = {
            "signalAtHead", "signalLeftBehind", "signalRescan", "signalAttempt",
            "signalNoParkedPre", "signalTokenBusy", "tokenAcquired", "tokenReleased",
            "searcherWoken", "searchFound", "searchEmpty", "chain", "preParkFound",
            "stolen", "relocated", "stealLockBusy", "signalNoParkedPost",
            "signalSuppressed", "searchRefused", "searchZeroGap", "preParkRefused",
            "marked", "wakeLt1us", "wakeLt10us", "wakeLt100us", "wakeGe100us"
        };
    }

    void count(int stat) {
        if (STEAL_STATS) {
            if (Thread.currentCarrierThread() instanceof CarrierThread ct && ct.scheduler == this) {
                ct.stats[stat]++;
            } else {
                externalStats.incrementAndGet(stat);
            }
        }
    }

    private void printStats() {
        var out = System.err;
        for (CarrierThread c : carriers) {
            StringBuilder sb = new StringBuilder("mpsc-steal-stats carrier=").append(c.id);
            for (int i = 0; i < Stat.COUNT; i++) {
                sb.append(' ').append(Stat.NAMES[i]).append('=').append(c.stats[i]);
            }
            out.println(sb);
        }
        StringBuilder sb = new StringBuilder("mpsc-steal-stats carrier=external");
        for (int i = 0; i < Stat.COUNT; i++) {
            sb.append(' ').append(Stat.NAMES[i]).append('=').append(externalStats.get(i));
        }
        out.println(sb);
    }

    private CarrierThread carrierFor() {
        Thread caller = Thread.currentCarrierThread();
        if (caller instanceof CarrierThread ct && ct.scheduler == this) {
            return ct;
        }
        return carriers[Math.floorMod(probe(), carriers.length)];
    }

    /**
     * Hands out the starting value of each platform thread's probe, so that different
     * submitting threads start on different carriers. A global sequence rather than
     * anything derived from the thread, for the reason ThreadLocalRandom.localInit()
     * uses one: thread ids are sequential, and a hash of a handful of them spreads
     * poorly over a small carrier count.
     */
    private static final java.util.concurrent.atomic.AtomicInteger PROBE_SEEDER =
            new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Returns the carrier index for external submissions from the calling platform
     * thread. Stable for the life of the thread: every virtual thread it starts goes to
     * the same carrier, and different threads are spread by the seeder.
     *
     * Stability is deliberate. A thread submitting a burst wakes its target carrier
     * once and then finds it RUNNING for the rest of the burst, so the burst costs one
     * eventfd write. Rotating the index instead sends consecutive tasks to different
     * carriers, each of which drains its share and re-parks before its next task
     * arrives, so nearly every submission finds a parked carrier and pays a wakeup.
     * This is the same reason ForkJoinPool keeps its probe stable and advances it only
     * on a failed queue acquisition. Callers that need their threads spread ask for it
     * with roundRobinAffinity(), which assigns carriers at the factory and does not
     * come through here.
     *
     * Only platform threads reach here: a submission from a virtual thread takes the
     * calling-carrier path in carrierFor().
     */
    private static int probe() {
        Thread t = Thread.currentThread();
        int p = t.mpscProbe;
        if (p == 0) {
            p = PROBE_SEEDER.incrementAndGet();
            t.mpscProbe = p;
        }
        return p;
    }

    // ---- Carrier thread ----

    static final class CarrierThread extends Thread {
        static final int RUNNING = 0;
        static final int PARKED  = 1;
        // STEAL only: SEARCHING + victim id, set by a waker on a PARKED carrier together
        // with the searcher token; consumed by the getAndSet in leaveParked()
        static final int SEARCHING = 2;

        static final VarHandle STATE;
        private static final VarHandle STEAL_LOCK;
        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                STATE = l.findVarHandle(CarrierThread.class, "carrierState", int.class);
                STEAL_LOCK = l.findVarHandle(CarrierThread.class, "stealLock", int.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        final int id;
        final MpscUnboundedQueue<VirtualThreadTask> queue = new MpscUnboundedQueue<>(64);
        final MpscVirtualThreadScheduler scheduler;

        // STEAL only: tasks that other carriers may take, consumed under stealLock
        final MpscUnboundedQueue<VirtualThreadTask> stealQueue =
                STEAL ? new MpscUnboundedQueue<>(64) : null;

        // STEAL_STATS only: written by this carrier thread only
        final long[] stats = STEAL_STATS ? new long[Stat.COUNT] : null;

        // STEAL only, this carrier as thief: the victim tail index at its previous
        // look at each carrier (0 = none; a tail of 0 has nothing queued before it)
        private final long[] lastSeenTail;

        // STEAL only, this carrier as searcher: the victim tail at the signaller's
        // look, written by the waker before its PARKED -> SEARCHING CAS
        long searchTail;
        long searchSignalNanos;

        // STEAL only, this carrier as victim: set by a thief whose look refused, i.e.
        // saw everything queued at its previous look already taken; leftBehind() then
        // stops signalling until this carrier's own poll finds its stealable queue
        // empty
        @Contended("stealLock")
        volatile boolean refusedMark;

        // carrier-local poller (Mode 4), null if using Mode 3 or lower
        CarrierLocalPoller poller;

        @Contended
        volatile int carrierState;

        // STEAL only: 1 while a consumer (this carrier or a thief) polls stealQueue
        @Contended("stealLock")
        volatile int stealLock;

        CarrierThread(int id, MpscVirtualThreadScheduler scheduler, int parallelism) {
            super(null, null, "mpsc-carrier-" + id, 0, false);
            this.id = id;
            this.scheduler = scheduler;
            this.lastSeenTail = STEAL ? new long[parallelism] : null;
            setDaemon(true);
        }

        @Override
        public void run() {
            if ("4".equals(System.getProperty("jdk.pollerMode"))) {
                try {
                    this.poller = new CarrierLocalPoller(STEAL);
                    if (STEAL) {
                        eventLoopSteal();
                    } else {
                        eventLoop();
                    }
                    return;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            if (STEAL) {
                plainLoopSteal();
            } else {
                plainLoop();
            }
        }

        /**
         * Mode 4: interleave task draining with I/O polling.
         * Like Netty's EventLoop: drain tasks → poll I/O → drain → ...
         * Block in epoll_wait only when both queue and I/O are idle.
         */
        private void eventLoop() {
            var poller = this.poller;
            for (;;) {
                int drained = drainRound();

                // non-blocking I/O poll
                int ioEvents = 0;
                try {
                    ioEvents = poller.poll(0);
                } catch (IOException e) { }

                if (drained + ioEvents > 0) {
                    continue;
                }

                // one more non-blocking check before parking
                try {
                    if (poller.poll(0) > 0) continue;
                } catch (IOException e) { }

                // genuinely idle: blocking poll
                carrierState = PARKED;

                VirtualThreadTask task = queue.poll();
                if (task != null) {
                    carrierState = RUNNING;
                    try { task.run(); } catch (Throwable t) { }
                    continue;
                }

                try {
                    poller.poll(-1);
                } catch (IOException e) { }
                carrierState = RUNNING;
            }
        }

        /**
         * Runs the tasks the queue held when the round began, and no more.
         *
         * The snapshot bounds the round without reading a clock. Tasks that arrive while
         * the round is running -- the continuations this round's own task.run() calls
         * submit, and the ones the following poll will unpark -- are left for the next
         * round, so arrivals can never keep the poll from being reached at all, which
         * draining to empty can.
         *
         * What it does not do is bound the round in time. The quota is the queue depth
         * at entry and nothing here bounds that, so a round lasts quota x service time
         * and the interval between polls grows with the backlog. That interval bounds
         * discovery of an fd that becomes ready mid-round: its continuation is enqueued
         * only once a poll sees it, so every task that runs before that poll is ahead of
         * it in the FIFO. A time budget does bound the interval while tasks are short --
         * it cannot bound a round holding one slow task, having no way to preempt a task
         * already running -- and charges a clock read per round to do it. Which bound is
         * better is a question about the depth distribution of this queue under a real
         * workload, and that has not been measured.
         *
         * Only not-yet-discovered readiness pays that cost. A continuation an earlier
         * poll already enqueued sits at the tail of this same FIFO, and ending the round
         * sooner does not move it ahead of anything queued before it.
         */
        private int drainRound() {
            var queue = this.queue;
            int quota = queue.size();
            int ran = 0;
            VirtualThreadTask task;
            while (ran < quota && (task = queue.poll()) != null) {
                try { task.run(); } catch (Throwable t) { }
                ran++;
            }
            return ran;
        }

        /**
         * Plain loop (Mode 3 or lower): poll tasks, park when idle.
         */
        private void plainLoop() {
            var queue = this.queue;
            for (;;) {
                VirtualThreadTask task = queue.poll();
                if (task != null) {
                    try { task.run(); } catch (Throwable t) { }
                    continue;
                }

                carrierState = PARKED;

                if ((task = queue.poll()) != null) {
                    carrierState = RUNNING;
                    try { task.run(); } catch (Throwable t) { }
                    continue;
                }

                LockSupport.park();
                carrierState = RUNNING;
            }
        }

        // ---- STEAL only ----

        /** eventLoop() with two queues and stealing. */
        private void eventLoopSteal() {
            var poller = this.poller;
            for (;;) {
                int drained = drainRoundSteal();

                int ioEvents = 0;
                try {
                    ioEvents = poller.poll(0);
                } catch (IOException e) { }

                if (drained + ioEvents > 0) {
                    continue;
                }

                try {
                    if (poller.poll(0) > 0) continue;
                } catch (IOException e) { }

                enterParked();
                if (workBeforeBlocking()) {
                    continue;
                }

                try {
                    poller.poll(-1);
                } catch (IOException e) { }
                leaveParked(true);
            }
        }

        /** plainLoop() with two queues and stealing. */
        private void plainLoopSteal() {
            for (;;) {
                if (drainRoundSteal() > 0) {
                    continue;
                }

                enterParked();
                if (workBeforeBlocking()) {
                    continue;
                }

                LockSupport.park();
                leaveParked(true);
            }
        }

        /**
         * drainRound() over both queues: the pinned queue's snapshot, then the
         * stealable queue's. Pinned first is a choice, not a requirement: each queue is
         * bounded by its own snapshot, so neither can starve the other. A thief may
         * take stealable tasks meanwhile, which ends that part of the round early.
         */
        private int drainRoundSteal() {
            var queue = this.queue;
            int quota = queue.size();
            int stealQuota = stealQueue.size();
            int ran = 0;
            VirtualThreadTask task;
            for (int i = 0; i < quota && (task = queue.poll()) != null; i++) {
                try { task.run(); } catch (Throwable t) { }
                ran++;
            }
            for (int i = 0; i < stealQuota && (task = pollStealable()) != null; i++) {
                try { task.run(); } catch (Throwable t) { }
                ran++;
            }
            return ran;
        }

        /**
         * Called with PARKED published. Rechecks both own queues, then takes one task
         * from another running carrier's stealable queue. Returns true if the caller
         * should loop instead of blocking; the state has then been left already.
         */
        private boolean workBeforeBlocking() {
            VirtualThreadTask task = queue.poll();
            if (task == null) {
                task = pollStealable();
            }
            if (task != null) {
                leaveParked(false);
                try { task.run(); } catch (Throwable t) { }
                return true;
            }
            CarrierThread victim = pickVictim();
            if (victim != null) {
                leaveParked(false);
                task = victim.trySteal();
                if (task != null) {
                    scheduler.count(Stat.PREPARK_FOUND);
                    victim.leftBehind();
                    runStolen(task);
                }
                return true;
            }
            return false;
        }

        /**
         * Looks at every other running carrier with stealable work, records each one's
         * tail for the next look, and returns the first whose work has waited since
         * this carrier's previous look at it (see STEAL), or null.
         */
        private CarrierThread pickVictim() {
            var carriers = scheduler.carriers;
            int n = carriers.length;
            CarrierThread found = null;
            for (int i = 1; i < n; i++) {
                CarrierThread c = carriers[(id + i) % n];
                if (c.carrierState != RUNNING) {
                    continue;
                }
                var q = c.stealQueue;
                long head = q.headIndex();
                long tail = q.tailIndex();
                if (head == tail) {
                    continue;
                }
                long seen = lastSeenTail[c.id];
                lastSeenTail[c.id] = tail;
                if (found == null && seen > head) {
                    found = c;
                } else if (found == null) {
                    scheduler.count(Stat.PREPARK_REFUSED);
                    if (seen != 0) {
                        c.mark();
                    }
                }
            }
            return found;
        }

        /** As victim: a thief's look refused; see refusedMark. */
        void mark() {
            if (STEAL_MARK && !refusedMark) {
                refusedMark = true;
                scheduler.count(Stat.MARKED);
            }
        }

        private void enterParked() {
            carrierState = PARKED;
            // after the PARKED store and before the queue rechecks; see signalWork
            PARKED_COUNT.getAndAdd(scheduler, 1);
        }

        /**
         * Leaves PARKED. A waker may have replaced PARKED with SEARCHING + victim at
         * any point after enterParked(), handing this carrier the searcher token; the
         * getAndSet sees that whichever way the carrier leaves, so the token cannot
         * be lost.
         */
        private void leaveParked(boolean blocked) {
            int prev = (int) STATE.getAndSet(this, RUNNING);
            PARKED_COUNT.getAndAdd(scheduler, -1);
            if (prev >= SEARCHING) {
                CarrierThread victim = scheduler.carriers[prev - SEARCHING];
                if (blocked) {
                    search(victim);
                } else {
                    // Moved to SEARCHING before it blocked: no wake ran between the
                    // signaller's look and this one, so there is no interval to judge
                    // by. Keep the signaller's look as this carrier's previous look.
                    scheduler.count(Stat.SEARCH_ZERO_GAP);
                    if (searchTail > lastSeenTail[victim.id]) {
                        lastSeenTail[victim.id] = searchTail;
                    }
                    scheduler.releaseToken(false);
                }
            }
        }

        /**
         * Runs as the searcher-token holder: steals one task from the victim, hands
         * the token to the next parked carrier if the victim still has stealable work,
         * otherwise releases it, then runs the task.
         */
        private void search(CarrierThread victim) {
            if (STEAL_STATS) {
                long ns = System.nanoTime() - searchSignalNanos;
                scheduler.count(ns < 1_000 ? Stat.WAKE_LT_1US
                        : ns < 10_000 ? Stat.WAKE_LT_10US
                        : ns < 100_000 ? Stat.WAKE_LT_100US : Stat.WAKE_GE_100US);
            }
            var q = victim.stealQueue;
            long seen = searchTail;
            if (victim.carrierState != RUNNING || q.headIndex() >= seen) {
                // everything queued at the signal has been taken during the wake, or
                // the victim parked (it drains its own queue): refuse, no rescan
                scheduler.count(Stat.SEARCH_REFUSED);
                lastSeenTail[victim.id] = q.tailIndex();
                victim.mark();
                scheduler.releaseToken(false);
                return;
            }
            // the signaller's look becomes this carrier's previous look at the victim,
            // so its pre-park look after running the stolen task can judge by it
            if (seen > lastSeenTail[victim.id]) {
                lastSeenTail[victim.id] = seen;
            }
            VirtualThreadTask task = victim.trySteal();
            if (task == null) {
                scheduler.count(Stat.SEARCH_EMPTY);
                scheduler.releaseToken(false);
                return;
            }
            scheduler.count(Stat.SEARCH_FOUND);
            VarHandle.fullFence();
            if (!victim.stealQueue.isEmpty() && scheduler.wakeSearcher(victim)) {
                scheduler.count(Stat.CHAIN);
            } else {
                scheduler.releaseToken(true);
            }
            runStolen(task);
        }

        private void runStolen(VirtualThreadTask task) {
            scheduler.count(Stat.STOLEN);
            if (STEAL_RELOCATE) {
                ((VirtualThread) task.thread()).affinityHint = id;
                scheduler.count(Stat.RELOCATED);
            }
            try { task.run(); } catch (Throwable t) { }
        }

        /** The owner's poll of its own stealable queue. */
        private VirtualThreadTask pollStealable() {
            while (!STEAL_LOCK.compareAndSet(this, 0, 1)) {
                Thread.onSpinWait();
            }
            VirtualThreadTask task = stealQueue.poll();
            STEAL_LOCK.setRelease(this, 0);
            if (task != null) {
                leftBehind();
            } else if (refusedMark) {
                refusedMark = false;
            }
            return task;
        }

        /**
         * Called by another carrier on this one (the victim): takes one task from the
         * stealable queue, or returns null if it is empty or its lock stays taken.
         */
        VirtualThreadTask trySteal() {
            var q = stealQueue;
            if (q.isEmpty()) {
                return null;
            }
            // try once: a held lock means a consumer is taking right now
            if (!STEAL_LOCK.compareAndSet(this, 0, 1)) {
                scheduler.count(Stat.STEAL_LOCK_BUSY);
                return null;
            }
            VirtualThreadTask task = q.poll();
            STEAL_LOCK.setRelease(this, 0);
            return task;
        }

        /**
         * After a consumer took a task from this carrier's stealable queue: if more is
         * queued behind it, signal a searcher for this carrier. The fence orders the
         * consumer-index store in poll() before the producer-index load in isEmpty();
         * a producer offering concurrently does the opposite pair (CAS on the producer
         * index, then a volatile load of the consumer index in enqueueStealable), so
         * either this sees its task or it sees its task at the head.
         */
        void leftBehind() {
            VarHandle.fullFence();
            if (!stealQueue.isEmpty()) {
                if (refusedMark) {
                    scheduler.count(Stat.SIGNAL_SUPPRESSED);
                    return;
                }
                scheduler.count(Stat.SIGNAL_LEFT_BEHIND);
                scheduler.signalWork(this, true);
            }
        }
    }

    @Override
    public String toString() {
        return "MpscVirtualThreadScheduler[carriers=" + carriers.length + "]";
    }
}
