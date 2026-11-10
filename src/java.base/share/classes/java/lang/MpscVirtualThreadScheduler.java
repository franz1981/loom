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
import sun.nio.ch.LocalPoller;
import sun.nio.ch.LocalPollers;

/**
 * A virtual thread scheduler with one MPSC queue per carrier (MpscCarrierThread).
 *
 * <p>Each virtual thread has a home, the carrier that every continuation of the
 * thread is submitted to. It is set on start: the starting carrier or, from a
 * platform thread, the carrier that thread's stable probe picks. A preferred carrier is
 * ignored. Without stealing the home never changes; with {@link #STEAL} a thief
 * becomes the home of the thread it steals.
 *
 * <p>Mode 4 (CARRIER_LOCAL_POLLER) is the default where available (Linux, with
 * continuations, jdk.pollerMode unset or 4): each carrier owns a LocalPoller that VT
 * fds register with directly, and alternates rounds of its queue with polls.
 * Mode 3 is not supported: jdk.pollerMode=3 falls back, with a warning, to the
 * platform's default poller mode.
 *
 * <p>A yield resubmits the thread. Without stealing, in Mode 4, its carrier holds it
 * and runs it at the end of the next round, after everything queued before it,
 * including what the poll woke. Otherwise it goes to the tail of its home queue; with
 * stealing it is never held, as a held thread is not in the queue thieves take from.
 * A lone yielder never makes its carrier block.
 *
 * <p>With stealing, a producer wakes the home carrier if it is parked, else signals
 * (SearchGate); the owner announces its own submissions before its next task if they
 * are not that task, and a WAKING owner announces what its waker deferred. See
 * MpscCarrierThread for the carrier states and SearchGate for the searcher token.
 */
final class MpscVirtualThreadScheduler
        implements VirtualThread.BuiltinScheduler, LocalPoller.Scheduler {

    /**
     * Opt-in work stealing; when off, there is no SearchGate or IdleSet and the carriers
     * run loops that never steal.
     */
    static final boolean STEAL = Boolean.getBoolean("jdk.virtualThreadScheduler.mpscSteal");

    private final MpscCarrierThread[] carriers;

    MpscVirtualThreadScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        LocalPoller[] pollers = createLocalPollers(parallelism);
        this.carriers = new MpscCarrierThread[parallelism];
        IdleSet idleSet = STEAL ? new IdleSet(parallelism) : null;
        SearchGate gate = STEAL ? new SearchGate(carriers, idleSet) : null;
        for (int i = 0; i < parallelism; i++) {
            carriers[i] = new MpscCarrierThread(i, this, idleSet, gate,
                    (pollers != null) ? pollers[i] : null);
        }
        for (int i = 0; i < parallelism; i++) {
            carriers[i].start();
        }
    }

    /** On failure, closes those created and throws: no carrier runs without its poller. */
    private static LocalPoller[] createLocalPollers(int count) {
        if (!LocalPollers.enabled()) {
            return null;
        }
        LocalPoller[] pollers = new LocalPoller[count];
        int created = 0;
        try {
            for (; created < count; created++) {
                pollers[created] = LocalPollers.create();
            }
        } catch (IOException e) {
            closeAll(pollers, created);
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            closeAll(pollers, created);
            throw e;
        }
        return pollers;
    }

    private static void closeAll(LocalPoller[] pollers, int count) {
        for (int i = 0; i < count; i++) {
            pollers[i].close();
        }
    }

    static final class MpscTask extends VirtualThread.VThreadTask {
        // plain: written on start before the first offer, by a submitter that moves the
        // thread before its offer, and by a thief before it runs the thread; a submitter
        // reads it after the thread's state change that follows that run
        private MpscCarrierThread home;

        MpscTask(VirtualThread vthread) {
            super(vthread);
        }
    }

    @Override
    public VirtualThread.VThreadTask newTask(VirtualThread vthread) {
        return new MpscTask(vthread);
    }

    // A custom scheduler's task keeps its home in builtinState.

    private static MpscCarrierThread home(VirtualThreadTask task) {
        if (task instanceof MpscTask t) {
            return t.home;
        }
        return (MpscCarrierThread) ((VirtualThread.CustomVThreadTask) task).builtinState;
    }

    static void setHome(VirtualThreadTask task, MpscCarrierThread home) {
        if (task instanceof MpscTask t) {
            t.home = home;
        } else {
            ((VirtualThread.CustomVThreadTask) task).builtinState = home;
        }
    }

    @Override
    public void onStart(VirtualThreadTask task) {
        MpscCarrierThread home = carrierFor();
        setHome(task, home);
        home.submit(task);
    }

    @Override
    public void onContinue(VirtualThreadTask task) {
        MpscCarrierThread home = home(task);
        if (home == null) {
            onStart(task);
            return;
        }
        // without stealing nothing would move the thread off a carrier whose waker
        // never unmounts, so only with stealing does a wake made on a carrier move it
        if (STEAL && Thread.currentCarrierThread() instanceof MpscCarrierThread ct
                && ct != home && ct.belongsTo(this)) {
            setHome(task, ct);
            home = ct;
        }
        boolean yielded = task instanceof VirtualThread.VThreadTask t && t.isYielded();
        // the carrier a thread yielded on resubmits it, and that carrier is its home
        assert !(yielded && task instanceof MpscTask) || Thread.currentThread() == home
                : "YIELDED thread submitted by " + Thread.currentThread();
        if (yielded) {
            home.resubmitYield(task);
        } else {
            home.submit(task);
        }
    }

    private MpscCarrierThread carrierFor() {
        Thread caller = Thread.currentCarrierThread();
        if (caller instanceof MpscCarrierThread ct && ct.belongsTo(this)) {
            return ct;
        }
        return carriers[Math.floorMod(probe(), carriers.length)];
    }

    // a sequence, as in ThreadLocalRandom.localInit: hashes of a few sequential thread
    // ids spread poorly over a few carriers
    private static final java.util.concurrent.atomic.AtomicInteger PROBE_SEEDER =
            new java.util.concurrent.atomic.AtomicInteger();

    /**
     * Stable for the calling platform thread's life, as ForkJoinPool's probe: a burst
     * then wakes its carrier once and finds it RUNNING after, where rotating would find
     * a parked carrier nearly every time.
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

    @Override
    public String toString() {
        return "MpscVirtualThreadScheduler[carriers=" + carriers.length + "]";
    }
}
