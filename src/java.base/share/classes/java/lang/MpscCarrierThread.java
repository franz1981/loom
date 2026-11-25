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
import jdk.internal.misc.BlockingCarrier;
import jdk.internal.vm.annotation.Contended;
import sun.nio.ch.LocalPoller;

import static java.lang.MpscVirtualThreadScheduler.STEAL;
import static java.lang.MpscVirtualThreadScheduler.setHome;

/**
 * A carrier thread of MpscVirtualThreadScheduler, in Mode 4 when poller is not null.
 *
 * <p>In Mode 4 the carrier polls its poller, with no thread mounted, only:
 * <ul>
 * <li>without blocking when its queue is empty;
 * <li>without blocking before it runs a task taken at the top of its loop, once
 *     {@code jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros} (default 500) has
 *     passed since its last poll, if fds are registered;
 * <li>without blocking before it queues a yielder that would be alone in its queue;
 * <li>blocking, on the park path.
 * </ul>
 * Readiness unparks threads on this carrier's own thread, so they are queued here. A
 * running task is never interrupted: ready I/O waits up to the interval plus that task.
 * A poll that fills its event array leaves the interval expired.
 *
 * <p>States of {@code carrierState}:
 * <pre>
 *   RUNNING -> PARKED                 the carrier
 *   PARKED  -> RUNNING                the carrier
 *   PARKED  -> WAKING                 a waker
 *   PARKED  -> SEARCHING + victim id  SearchGate's token holder
 *   WAKING, SEARCHING -> RUNNING      the carrier
 * </pre>
 * WAKING: a wake is in flight; producers that read it do not signal, the carrier
 * announces its backlog once it runs. SEARCHING: the carrier holds the token and steals
 * from the victim.
 *
 * <p>Invariants:
 * <ul>
 * <li>A task offered to a parked carrier is never missed, and a park costs at most
 *     one wake.
 * <li>No task runs while the state reads PARKED.
 * <li>The queue is one live FIFO: a task runs after everything queued on this carrier
 *     before it, unless a thief takes it first. A runnable thread is in exactly one
 *     carrier's queue or is a task in hand.
 * <li>The own thread offers only with no thread mounted; run(task) is the only place a
 *     task runs.
 * <li>With stealing, a task is announced unless it is the next task its owner runs: by
 *     its producer when that is another thread, else by the owner before its next task
 *     (or after WAKING).
 * </ul>
 *
 * <p>Memory order: enterParked's volatile store of PARKED, then the recheck's volatile
 * read of the producer index, pairs with submit's producer-index CAS, then its volatile
 * read of carrierState.
 *
 * <p>A carrier is a BlockingCarrier: a virtual thread that begins a Blocker section
 * here moves to an offload pool thread for the section ({@link MpscOffloadPool}). It is
 * then in no carrier's queue, and comes back home through its scheduler, as a wake from
 * another thread.
 */
final class MpscCarrierThread extends Thread implements LocalPoller.Carrier, BlockingCarrier {
    private static final VarHandle STATE;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            STATE = l.findVarHandle(MpscCarrierThread.class, "carrierState", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final long MAX_POLL_INTERVAL_NANOS = TimeUnit.MICROSECONDS.toNanos(Math.max(0L,
            Long.getLong("jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros", 500L)));

    final int id;
    // chunks double up to the largest capacity, as ForkJoinPool's work queues grow
    private final RunQueue<VirtualThreadTask> queue =
            new RunQueue<>(64, RunQueue.MAX_CHUNK_CAPACITY);
    private final MpscVirtualThreadScheduler scheduler;
    private final IdleSet idleSet;
    private final SearchGate gate;

    private final LocalPoller poller;

    @Contended("local")
    private long lastPoll;

    @Contended("state")
    private volatile int carrierState;

    @Contended("local")
    private int xorshift;

    // an own-thread offer since the last task ran; read only with stealing, in run(task)
    @Contended("local")
    private boolean unannounced;

    // a pinned thread's Blocker section runs here: set at its begin, cleared at its end,
    // and no other thread runs here in between
    @Contended("local")
    private boolean pinnedSection;

    MpscCarrierThread(ThreadGroup group, int id, MpscVirtualThreadScheduler scheduler,
                      IdleSet idleSet, SearchGate gate, LocalPoller poller) {
        super(group, null, "mpsc-carrier-" + id, 0, false);
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
    private static final int WAKING = 2;
    private static final int SEARCHING = 3;

    private int wakeParked() {
        int witness = (int) STATE.compareAndExchange(this, PARKED, WAKING);
        if (witness == PARKED) {
            wake();
        }
        return witness;
    }

    /** A failed eventfd write is a JDK bug: only the pollers of carriers never started close. */
    private void wake() {
        if (poller != null) {
            try {
                poller.wakeup();
            } catch (IOException e) {
                throw new InternalError(e);
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
        if (STEAL) {
            idleSet.setIdle(id);
        }
    }

    /** After the blocking poll or park. */
    private void leaveParked() {
        int prev = (int) STATE.getAndSet(this, RUNNING);
        if (STEAL) {
            idleSet.clearIdle(id);
            if (prev == WAKING) {
                announceBacklog(false);
            } else if (prev >= SEARCHING) {
                search(gate.carrier(prev - SEARCHING));
            }
        }
    }

    /** Leaves PARKED without blocking, with a task in hand. */
    private void abortPark() {
        int prev = (int) STATE.getAndSet(this, RUNNING);
        if (STEAL) {
            idleSet.clearIdle(id);
            if (prev == WAKING) {
                announceBacklog(true);
            } else if (prev >= SEARCHING) {
                gate.release(nextRandom());
            }
        }
    }

    // ---- Used by the scheduler and SearchGate ----

    /**
     * Queues a task here. The own thread wakes nothing: it offers from its loop or from
     * its own blocking poll, and takes the task when it returns there.
     */
    void submit(VirtualThreadTask task) {
        queue.offer(task);
        if (Thread.currentThread() == this) {
            unannounced = true;
            return;
        }
        int state = carrierState;
        if (state == PARKED) {
            // witness WAKING: the carrier announces once it runs; SEARCHING: it holds
            // the token, whose release rescans
            if (wakeParked() == RUNNING && STEAL && hasQueued()) {
                gate.signal(this);
            }
        } else if (STEAL && state != WAKING) {
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
            // the yielder is not queued yet, so no thief can run it during the poll
            pollNoWait();
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

    // ---- Blocker sections ----

    /**
     * Invoked by the virtual thread mounted here as it begins a Blocker section: sets
     * its task's offload hint and yields, and the scheduler's onContinue, on this
     * carrier, hands the unmounted thread to the offload pool. Returns false, with no
     * yield, if the pool has no thread to give or the thread is a custom scheduler's:
     * the section runs here. A hint still set after the yield means the thread could
     * not unmount (pinned): the section runs here and endBlocking reports it. This
     * relies on Thread.yield() always trying to unmount. An OutOfMemoryError from the
     * yield propagates, as a failed compensation does on a CarrierThread.
     */
    @Override
    public boolean beginBlocking() {
        MpscOffloadPool pool = scheduler.offloadPool();
        if (pool == null || !pool.mayTake()
                || !(((VirtualThread) Thread.currentThread()).virtualThreadTask()
                        instanceof MpscVirtualThreadScheduler.MpscTask task)) {
            return false;
        }
        boolean stayed;
        task.offload = true;
        try {
            Thread.yield();
        } finally {
            stayed = task.offload;
            task.offload = false;
        }
        if (stayed) {
            pinnedSection = true;
        }
        return true;
    }

    /**
     * Invoked when a section ends on this carrier. A section a pinned thread ran here is
     * recorded as a jdk.VirtualThreadPinned event, from the failed yield to now. Any
     * other thread is back on a carrier after its yield at the begin (no pool thread
     * took it, or it was woken onto a carrier from its pool thread): nothing to undo.
     */
    @Override
    public void endBlocking() {
        if (pinnedSection) {
            pinnedSection = false;
            VirtualThread.postPinnedEvent("Blocking operation");
        }
    }

    // ---- The carrier's own loops ----

    @Override
    public void run() {
        for (;;) {
            VirtualThreadTask task = pollShared();
            if (task != null) {
                if (poller != null && pollDue()) {
                    pollNoWait();
                }
                run(task);
                continue;
            }

            if (poller != null) {
                pollNoWait();
                if (hasQueued()) {
                    continue;
                }
            }

            enterParked();
            if (workBeforeBlocking()) {
                continue;
            }

            block();
            leaveParked();
        }
    }

    /**
     * With stealing, first announces the own thread's offers if any is still queued. A
     * task's error goes to the uncaught-exception handler and the carrier goes on:
     * ending it would strand its queue.
     */
    private void run(VirtualThreadTask task) {
        if (STEAL && unannounced) {
            unannounced = false;
            if (hasQueued()) {
                gate.signal(this);
            }
        }
        try {
            task.run();
        } catch (Throwable t) {
            reportUncaught(t);
        }
    }

    private void reportUncaught(Throwable t) {
        try {
            dispatchUncaughtException(t);
        } catch (Throwable ignore) {
            // the handler's own error is dropped
        }
    }

    /**
     * Still PARKED: rechecks its own queue, then, with stealing, tries one steal; false
     * to block.
     */
    private boolean workBeforeBlocking() {
        VirtualThreadTask task = pollShared();
        if (task != null) {
            abortPark();
            run(task);
            return true;
        }
        if (!STEAL) {
            return false;
        }
        MpscCarrierThread victim = gate.bestVictim(this, nextRandom());
        if (victim != null && (task = victim.trySteal()) != null) {
            abortPark();
            if (victim.hasQueued()) {
                gate.signal(victim);
            }
            runStolen(task);
            return true;
        }
        // one look only: a task whose producer found no carrier idle can wait behind a
        // running task until the next signal
        return false;
    }

    /** Still PARKED: blocks in its poller (Mode 4) or parks, until woken. */
    private void block() {
        if (poller != null) {
            polled(poll(-1));
        } else {
            parkIdle();
        }
    }

    /** Mode 4, with a task in hand: the poll interval has passed. */
    private boolean pollDue() {
        return poller.hasRegistrations()
                && System.nanoTime() - lastPoll >= MAX_POLL_INTERVAL_NANOS;
    }

    /**
     * Mode 4, with no thread mounted: polls without blocking. A thread still parking on
     * another carrier when its fd is ready is queued by that carrier's resubmit.
     */
    private void pollNoWait() {
        // only this carrier registers, so no registration can appear during the poll
        if (poller.hasRegistrations()) {
            polled(poll(0));
        }
    }

    /**
     * Mode 4. The poll returns on EINTR, so an error is a JDK bug: it ends this carrier
     * and strands its queue.
     */
    private boolean poll(int timeout) {
        try {
            return poller.poll(timeout);
        } catch (IOException e) {
            throw new InternalError(e);
        }
    }

    private void polled(boolean filled) {
        long now = System.nanoTime();
        // a full event array may have left readiness behind: the next boundary polls again
        lastPoll = filled ? now - MAX_POLL_INTERVAL_NANOS : now;
    }

    /**
     * Clears the interrupt status after the park: no task can observe it, and left set
     * it would make every later park return at once.
     */
    private static void parkIdle() {
        LockSupport.park();
        Thread.interrupted();
    }

    private VirtualThreadTask pollShared() {
        return queue.take(true);
    }

    // ---- STEAL only ----

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

    // ---- The queue as other carriers and SearchGate see it ----

    int queuedEstimate() {
        return queue.sizeEstimate();
    }

    boolean hasQueued() {
        return !queue.isEmpty();
    }

    private VirtualThreadTask trySteal() {
        return queue.take(false);
    }
}
