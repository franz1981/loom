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
import java.lang.Thread.VirtualThreadTask;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import jdk.internal.vm.annotation.Contended;
import sun.nio.ch.LocalPoller;

import static java.lang.MpscVirtualThreadScheduler.STEAL;
import static java.lang.MpscVirtualThreadScheduler.setHome;

/**
 * A carrier thread of MpscVirtualThreadScheduler; localPoller() implements
 * LocalPoller.Carrier for sun.nio.ch.Poller. Mode 4 when poller is not null.
 *
 * <p>The carrier runs its queue as one live FIFO. In Mode 4 it polls its poller, with
 * no thread mounted, only: without blocking when its queue is empty; without blocking
 * before it runs a task it took from its queue, once
 * {@code jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros} (default 500) has passed
 * since its last poll, if fds are registered; without blocking before it queues a
 * yielder that would be alone in its queue; and blocking, on the park path.
 * Readiness unparks threads on this carrier's own thread, so they are queued here.
 * The interval is checked only between tasks: a running task is never interrupted, so
 * ready I/O waits up to the interval plus the task running when it expires, and that
 * task's run time has no bound.
 *
 * <p>States of {@code carrierState}:
 * <pre>
 *   RUNNING -> PARKED                 the carrier
 *   PARKED  -> RUNNING                the carrier
 *   PARKED  -> WAKING                 a waker
 *   PARKED  -> SEARCHING + victim id  SearchGate's token holder
 *   WAKING, SEARCHING -> RUNNING      the carrier
 * </pre>
 *
 * <p>Invariants:
 * <ul>
 * <li>A task offered to a parked carrier is never missed, and a park costs at most
 *     one wake.
 * <li>No task runs while the state reads PARKED.
 * <li>The queue is one live FIFO: a task runs after everything queued on this
 *     carrier before it, unless a thief takes it first. A runnable thread is in
 *     exactly one carrier's queue or is a task in hand; none is held aside.
 * <li>Mode 4: every task this carrier takes from its queue starts after a check of
 *     the poll interval, so a fd ready on its poller is polled at the first task
 *     boundary after the interval expires, or sooner when the queue empties; a poll
 *     that fills its event array leaves the interval expired.
 * <li>With stealing, a task is announced unless it is the next task its owner runs:
 *     by its producer when that is another thread (or by the owner after WAKING, for
 *     it), else by the owner before it runs the next task.
 * <li>With stealing the queue is shared: the owner (pollShared) and thieves
 *     (trySteal) consume it in turns under stealLock.
 * <li>With stealing, the owner's own thread offers tasks only with no thread mounted
 *     (after a continuation unmounts, or from its polls), never from a task's code;
 *     run(task) is the only place a task runs.
 * </ul>
 */
final class MpscCarrierThread extends Thread implements LocalPoller.Carrier {
    private static final VarHandle STATE;
    private static final VarHandle STEAL_LOCK;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(MpscCarrierThread.class, "carrierState", int.class);
            STEAL_LOCK = l.findVarHandle(MpscCarrierThread.class, "stealLock", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Mode 4: how long a carrier whose queue does not empty runs tasks before it
     * polls its poller, checked before each task it takes from its queue.
     */
    private static final long MAX_POLL_INTERVAL_NANOS = TimeUnit.MICROSECONDS.toNanos(Math.max(0L,
            Long.getLong("jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros", 500L)));

    final int id;
    private final MpscUnboundedQueue<VirtualThreadTask> queue = new MpscUnboundedQueue<>(64);
    private final MpscVirtualThreadScheduler scheduler;
    private final IdleSet idleSet;
    private final SearchGate gate;

    private final LocalPoller poller;

    // System.nanoTime() of the last poll that reached the poller
    @Contended("local")
    private long lastPoll;

    @Contended("state")
    private volatile int carrierState;

    @Contended("stealLock")
    private volatile int stealLock;

    @Contended("local")
    private int xorshift;

    // an own-thread offer since the last task ran: run(task) announces what is behind it
    @Contended("local")
    private boolean unannounced;

    MpscCarrierThread(int id, MpscVirtualThreadScheduler scheduler, IdleSet idleSet,
                      SearchGate gate, LocalPoller poller) {
        super(null, null, "mpsc-carrier-" + id, 0, false);
        this.id = id;
        this.scheduler = scheduler;
        this.idleSet = idleSet;
        this.gate = gate;
        this.poller = poller;
        this.xorshift = (id + 1) * 0x9E3779B9 | 1;
        this.lastPoll = System.nanoTime();
        setDaemon(true);
    }

    // ---- Carrier state ----

    private static final int RUNNING = 0;
    private static final int PARKED  = 1;
    // A waker claimed this park and its wake is in flight. Producers that read it
    // don't signal: the carrier announces its backlog once it runs, so a burst to a
    // parked carrier costs the producer one wake and the carrier wakes the helpers.
    private static final int WAKING = 2;
    private static final int SEARCHING = 3;

    private int wakeParked() {
        int witness = (int) STATE.compareAndExchange(this, PARKED, WAKING);
        if (witness == PARKED) {
            wake();
        }
        return witness;
    }

    private void wake() {
        if (poller != null) {
            try {
                poller.wakeup();
            } catch (IOException e) {
                LockSupport.unpark(this);
            }
        } else {
            LockSupport.unpark(this);
        }
    }

    boolean isParked() {
        return carrierState == PARKED;
    }

    boolean tryStartSearch(MpscCarrierThread victim) {
        if (STATE.compareAndSet(this, PARKED, SEARCHING + victim.id)) {
            wake();
            return true;
        }
        return false;
    }

    private void enterParked() {
        carrierState = PARKED;
        idleSet.setIdle(this);
    }

    /** After the blocking poll or park. */
    private void leaveParked() {
        int prev = (int) STATE.getAndSet(this, RUNNING);
        idleSet.clearIdle(this);
        if (prev == WAKING) {
            announceBacklog(false);
        } else if (prev >= SEARCHING) {
            search(gate.carrier(prev - SEARCHING));
        }
    }

    /** Leaves PARKED without blocking, with a task in hand. */
    private void abortPark() {
        int prev = (int) STATE.getAndSet(this, RUNNING);
        idleSet.clearIdle(this);
        if (prev == WAKING) {
            announceBacklog(true);
        } else if (prev >= SEARCHING) {
            gate.release(nextRandom());
        }
    }

    // ---- Used by the scheduler and SearchGate ----

    void submit(VirtualThreadTask task) {
        if (STEAL) {
            offerShared(task);
        } else {
            offer(task);
        }
    }

    private void offer(VirtualThreadTask task) {
        queue.offer(task);
        if (carrierState == PARKED) {
            // readiness from this carrier's own blocking poll, which drains on return
            if (Thread.currentCarrierThread() == this) {
                return;
            }
            wakeParked();
        }
    }

    private void offerShared(VirtualThreadTask task) {
        queue.offer(task);
        if (Thread.currentThread() == this) {
            unannounced = true;
            return;
        }
        int state = carrierState;
        if (state == PARKED) {
            if (wakeParked() == RUNNING && hasQueued()) {
                gate.signal(this);
            }
        } else if (state != WAKING) {
            gate.signal(this);
        }
    }

    boolean belongsTo(MpscVirtualThreadScheduler s) {
        return scheduler == s;
    }

    boolean usesGate(SearchGate g) {
        return gate == g;
    }

    /**
     * Submits a yielded thread. In Mode 4, on this carrier, a yielder that would be
     * alone in the queue is queued behind the I/O that is ready now.
     */
    void resubmitYield(VirtualThreadTask task) {
        if (poller != null && Thread.currentThread() == this && !hasQueued()) {
            // inside the yielder's afterYield: it is not queued yet, so no thief can
            // run it while this carrier polls
            admit();
        }
        submit(task);
    }

    int nextRandom() {
        int r = xorshift;
        r ^= r << 13;
        r ^= r >>> 17;
        r ^= r << 5;
        return xorshift = r;
    }

    @Override
    public LocalPoller localPoller() {
        return poller;
    }

    // ---- The carrier's own loops ----

    @Override
    public void run() {
        if (poller != null) {
            if (STEAL) {
                eventLoopSteal();
            } else {
                eventLoop();
            }
        } else if (STEAL) {
            plainLoopSteal();
        } else {
            plainLoop();
        }
    }

    private void eventLoop() {
        var queue = this.queue;
        var poller = this.poller;
        for (;;) {
            VirtualThreadTask task = queue.poll();
            if (task != null) {
                if (pollDue()) {
                    admit();
                }
                try { task.run(); } catch (Throwable t) { }
                continue;
            }

            if (admit() > 0) {
                continue;
            }

            carrierState = PARKED;

            if ((task = queue.poll()) != null) {
                carrierState = RUNNING;
                try { task.run(); } catch (Throwable t) { }
                continue;
            }

            try {
                poller.poll(-1);
            } catch (IOException e) { }
            polled();
            carrierState = RUNNING;
        }
    }

    /** Mode 4, with a task in hand: the poll interval has passed. */
    private boolean pollDue() {
        return poller.hasRegistrations()
                && System.nanoTime() - lastPoll >= MAX_POLL_INTERVAL_NANOS;
    }

    /**
     * Mode 4, on this carrier with no thread mounted: polls without blocking and
     * returns the number of threads unparked. Readiness wakes this carrier's own
     * thread, so the unparked threads are queued here.
     */
    private int admit() {
        // only this carrier registers, so no registration can appear during the poll
        if (!poller.hasRegistrations()) {
            return 0;
        }
        int unparked = 0;
        try {
            unparked = poller.poll(0);
        } catch (IOException e) { }
        polled();
        return unparked;
    }

    private void polled() {
        long now = System.nanoTime();
        // a full event array may have left readiness behind: the next boundary polls again
        lastPoll = poller.lastPollFilled() ? now - MAX_POLL_INTERVAL_NANOS : now;
    }

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

    private void eventLoopSteal() {
        var poller = this.poller;
        for (;;) {
            VirtualThreadTask task = pollShared();
            if (task != null) {
                if (pollDue()) {
                    admit();
                }
                run(task);
                continue;
            }

            if (admit() > 0) {
                continue;
            }

            enterParked();
            if (workBeforeBlocking()) {
                continue;
            }

            try {
                poller.poll(-1);
            } catch (IOException e) { }
            polled();
            leaveParked();
        }
    }

    private void plainLoopSteal() {
        for (;;) {
            VirtualThreadTask task = pollShared();
            if (task != null) {
                run(task);
                continue;
            }

            enterParked();
            if (workBeforeBlocking()) {
                continue;
            }

            LockSupport.park();
            leaveParked();
        }
    }

    /**
     * With stealing, the only place a task runs: a run site that bypasses it leaves the
     * owner's own offers unannounced. Those offers are announced here if anything
     * is still queued behind this task; a lone task finds the queue empty after its
     * own take and stays with its owner.
     */
    private void run(VirtualThreadTask task) {
        if (unannounced) {
            unannounced = false;
            if (hasQueued()) {
                gate.signal(this);
            }
        }
        try { task.run(); } catch (Throwable t) { }
    }

    /** Still PARKED: rechecks its own queue, then tries one steal; false to block. */
    private boolean workBeforeBlocking() {
        VirtualThreadTask task = pollShared();
        if (task != null) {
            abortPark();
            run(task);
            return true;
        }
        MpscCarrierThread victim = gate.bestVictim(this, nextRandom());
        if (victim != null && (task = victim.trySteal()) != null) {
            abortPark();
            // no fence before the emptiness test, as in SearchGate.handOn
            if (victim.hasQueued()) {
                gate.signal(victim);
            }
            runStolen(task);
            return true;
        }
        return false;
    }

    /** Keeps one task of the backlog for itself unless it has a task in hand. */
    private void announceBacklog(boolean taskInHand) {
        int backlog = queuedEstimate();
        if (backlog > 1 || (backlog == 1 && taskInHand)) {
            gate.signal(this);
        }
    }

    private void search(MpscCarrierThread victim) {
        VirtualThreadTask task = victim.trySteal();
        if (task == null) {
            gate.release(nextRandom());
            return;
        }
        gate.handOn(victim, nextRandom());
        runStolen(task);
    }

    private void runStolen(VirtualThreadTask task) {
        // before the run: the thread may park during it, and its wakeup goes to its home
        setHome(task, this);
        run(task);
    }

    private VirtualThreadTask pollShared() {
        while (!STEAL_LOCK.compareAndSet(this, 0, 1)) {
            Thread.onSpinWait();
        }
        VirtualThreadTask task = queue.poll();
        STEAL_LOCK.setRelease(this, 0);
        return task;
    }

    // ---- The queue as other carriers and SearchGate see it ----

    int queuedEstimate() {
        return queue.sizeEstimate();
    }

    boolean hasQueued() {
        return !queue.isEmpty();
    }

    private VirtualThreadTask trySteal() {
        var q = queue;
        if (q.isEmpty()) {
            return null;
        }
        if (!STEAL_LOCK.compareAndSet(this, 0, 1)) {
            return null;
        }
        VirtualThreadTask task = q.poll();
        STEAL_LOCK.setRelease(this, 0);
        return task;
    }
}
