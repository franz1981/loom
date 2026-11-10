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
 * <p>Each virtual thread has a home, a carrier and one of its queues, set on start
 * and used by every continuation. A thread starts on the starting carrier or, from
 * a platform thread, on the carrier that thread's stable probe picks. A preferred
 * carrier is ignored.
 *
 * <p>Mode 4 (CARRIER_LOCAL_POLLER) is the default where available (Linux, with
 * continuations, jdk.pollerMode unset or 4): each carrier owns a LocalPoller that VT
 * fds register with directly, and alternates rounds of its queues with polls.
 * Mode 3 is not supported: jdk.pollerMode=3 falls back, with a warning, to the
 * platform's default poller mode.
 *
 * <p>A yield resubmits the thread. Without stealing, in Mode 4 a thread is held by
 * its carrier and runs at the end of the next round, after everything queued before
 * it, including what the poll woke; any other goes to the tail of its home queue. A
 * lone yielder never makes its carrier block.
 *
 * <p>With {@link #STEAL}, every thread's home is its carrier's stealable
 * queue, and then the last carrier that took it. A producer of a stealable task wakes
 * the owner if it is parked, else signals (SearchGate); the owner announces its own
 * submissions before its next task if they are not that task, and a WAKING owner
 * announces what its waker deferred. See MpscCarrierThread for the carrier states and
 * SearchGate for the searcher token.
 */
final class MpscVirtualThreadScheduler
        implements VirtualThread.BuiltinScheduler, LocalPoller.Scheduler {

    /** Opt-in work stealing; when off, none of the code that tests it runs. */
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
        // written before each offer, which publishes it to the next submitter
        private MpscCarrierThread home;

        // link in the held yields of the carrier the thread yielded on (holdYield)
        MpscTask nextYielded;

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
        // holdYield relies on this
        assert !(task instanceof MpscTask t && t.isYielded())
                || Thread.currentThread() == home
                : "YIELDED thread submitted by " + Thread.currentThread();
        // never held with stealing: a held thread is not in the queue, where a thief takes it
        if (STEAL || !home.holdYield(task)) {
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
