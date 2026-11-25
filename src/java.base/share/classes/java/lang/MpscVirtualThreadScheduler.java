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
import java.lang.Thread.VirtualThreadTask;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.vm.VirtualThreadSchedulerStatistics;
import sun.nio.ch.LocalPoller;
import sun.nio.ch.LocalPollers;

/**
 * A virtual thread scheduler with one run queue per carrier (MpscCarrierThread), which
 * thieves also take from ({@link #STEAL}).
 *
 * <p>Each virtual thread has a home, the carrier every continuation of it is submitted
 * to. The home is set on start: the starting carrier or, from another thread, the
 * carrier that thread's stable probe picks; a preferred carrier is ignored. Without
 * stealing the home never changes. With {@link #STEAL} it moves in two ways: a thief
 * becomes the home of the thread it steals, before it runs it; and a wake made on a
 * carrier of this scheduler, by a virtual thread mounted there or by the carrier's own
 * thread, makes that carrier the home. A wake from any other thread goes to the home.
 * A yield resubmits the thread to the tail of its home's queue.
 *
 * <p>Mode 4 (CARRIER_LOCAL_POLLER) is the default where available (Linux, with
 * continuations, jdk.pollerMode unset or 4): each carrier owns a LocalPoller that VT
 * fds register with directly. Mode 3 is not supported: jdk.pollerMode=3 falls back,
 * with a warning, to the platform's default poller mode.
 *
 * <p>The carrier states, polling and announcing are in {@link MpscCarrierThread}, the
 * searcher token in {@link SearchGate}.
 */
final class MpscVirtualThreadScheduler
        implements VirtualThread.BuiltinScheduler, LocalPoller.Scheduler,
                   VirtualThreadSchedulerStatistics {

    /**
     * Work stealing, on unless jdk.virtualThreadScheduler.mpscSteal is false. Off is a
     * diagnostic setting with no SearchGate or IdleSet: a thread runs only on its home,
     * so a pinned thread that waits for a thread queued on its own carrier never resumes.
     */
    static final boolean STEAL = Boolean.parseBoolean(
            System.getProperty("jdk.virtualThreadScheduler.mpscSteal", "true"));

    // a sequence, as in ThreadLocalRandom.localInit: hashes of a few sequential thread
    // ids spread poorly over a few carriers
    private static final AtomicInteger PROBE_SEEDER = new AtomicInteger();

    private final MpscCarrierThread[] carriers;

    MpscVirtualThreadScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        LocalPoller[] pollers = createLocalPollers(parallelism);
        int started = 0;
        try {
            this.carriers = new MpscCarrierThread[parallelism];
            IdleSet idleSet = STEAL ? new IdleSet(parallelism) : null;
            SearchGate gate = STEAL ? new SearchGate(carriers, idleSet) : null;
            for (int i = 0; i < parallelism; i++) {
                carriers[i] = new MpscCarrierThread(i, this, idleSet, gate,
                        (pollers != null) ? pollers[i] : null);
            }
            for (; started < parallelism; started++) {
                carriers[started].start();
            }
        } catch (Throwable e) {
            // the carriers started stay parked: stopping them needs a shutdown path
            // that this scheduler does not have
            if (pollers != null) {
                closeAll(pollers, started, parallelism);
            }
            throw e;
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
            closeAll(pollers, 0, created);
            throw new UncheckedIOException(e);
        } catch (Throwable e) {
            closeAll(pollers, 0, created);
            throw e;
        }
        return pollers;
    }

    private static void closeAll(LocalPoller[] pollers, int from, int to) {
        for (int i = from; i < to; i++) {
            pollers[i].close();
        }
    }

    static final class MpscTask extends VirtualThread.VThreadTask {
        // plain: every write comes before the offer or run of the thread, and onContinue
        // reads it after the thread's next state change, which follows that run
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
            // a custom scheduler's thread that this scheduler did not start: the custom
            // scheduler delegates its continue here (external view) but ran its start
            // itself. It is placed as a start.
            onStart(task);
            return;
        }
        boolean yielded = task instanceof VirtualThread.VThreadTask t && t.isYielded();
        assert !(yielded && task instanceof MpscTask) || Thread.currentThread() == home
                : "YIELDED thread submitted by " + Thread.currentThread();
        // without stealing, nothing would move the thread off a waker's carrier again
        if (STEAL && Thread.currentCarrierThread() instanceof MpscCarrierThread ct
                && ct != home && ct.belongsTo(this)) {
            setHome(task, ct);
            home = ct;
        }
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

    /**
     * Stable for the calling thread's life, as ForkJoinPool's probe: a burst wakes its
     * carrier once rather than a parked carrier per submission.
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

    // ---- VirtualThreadSchedulerMXBean values: racy snapshots, as ForkJoinPool's ----

    @Override
    public int getParallelism() {
        return carriers.length;
    }

    @Override
    public int getPoolSize() {
        int size = 0;
        for (MpscCarrierThread c : carriers) {
            if (c.isAlive()) {
                size++;
            }
        }
        return size;
    }

    /**
     * The live carriers that are not parked; one between tasks or polling is counted
     * too, so this overestimates.
     */
    @Override
    public int getMountedVirtualThreadCount() {
        int count = 0;
        for (MpscCarrierThread c : carriers) {
            if (c.isAlive() && !c.isParked()) {
                count++;
            }
        }
        return count;
    }

    @Override
    public long getQueuedVirtualThreadCount() {
        long count = 0;
        for (MpscCarrierThread c : carriers) {
            count += c.queuedEstimate();
        }
        return count;
    }

    @Override
    public String toString() {
        return "MpscVirtualThreadScheduler[carriers=" + carriers.length + "]";
    }
}
