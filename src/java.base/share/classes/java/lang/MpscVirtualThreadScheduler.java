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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;
import jdk.internal.misc.Unsafe;
import jdk.internal.vm.annotation.Contended;
import sun.nio.ch.CarrierLocalPoller;

/**
 * An alternative virtual thread scheduler using a single MPSC queue per carrier.
 * By default each carrier drains only its own queue. An experimental opt-in mode
 * allows stealable virtual threads to spill to a shared MPMC queue while sticky
 * and round-robin-affined virtual threads remain carrier-local.
 *
 * <p>External submissions use a probe-based hash (FJP-style) to distribute
 * across carriers. Sticky and round-robin-affined virtual threads keep a
 * carrier-local affinity; ordinary virtual threads may spill to the shared
 * queue when work stealing is enabled and the local queue is above threshold.
 *
 * <p>With poller Mode 4 (CARRIER_LOCAL_POLLER), each carrier owns its own
 * epoll fd. VT fds register directly — no sub-pollers, no master poller.
 * The carrier interleaves task draining with I/O polling.
 */
final class MpscVirtualThreadScheduler implements VirtualThreadScheduler {

    private static final Unsafe U = Unsafe.getUnsafe();
    private static final String WORK_STEALING_PROPERTY =
            "jdk.virtualThreadScheduler.mpsc.workStealing";
    private static final String STEALABLE_THRESHOLD_PROPERTY =
            "jdk.virtualThreadScheduler.mpsc.localStealableThreshold";
    private static final int DEFAULT_LOCAL_STEALABLE_THRESHOLD = 64;
    private static final int SHARED_POLL_INTERVAL = 63;

    private static final long PROBE =
            U.objectFieldOffset(Thread.class, "threadLocalRandomProbe");

    private final CarrierThread[] carriers;
    private final boolean workStealingEnabled;
    private final int localStealableThreshold;
    private final ConcurrentLinkedQueue<VirtualThreadTask> sharedQueue;

    MpscVirtualThreadScheduler(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1");
        }
        boolean workStealingEnabled = Boolean.getBoolean(WORK_STEALING_PROPERTY);
        this.workStealingEnabled = workStealingEnabled;
        this.localStealableThreshold = workStealingEnabled
                ? Math.max(0, Integer.getInteger(STEALABLE_THRESHOLD_PROPERTY,
                        DEFAULT_LOCAL_STEALABLE_THRESHOLD))
                : Integer.MAX_VALUE;
        this.sharedQueue = workStealingEnabled ? new ConcurrentLinkedQueue<>() : null;
        this.carriers = new CarrierThread[parallelism];
        for (int i = 0; i < parallelism; i++) {
            carriers[i] = new CarrierThread(i, this);
        }
        for (int i = 0; i < parallelism; i++) {
            carriers[i].start();
        }
    }

    @Override
    public void onStart(VirtualThreadTask task) {
        VirtualThread vt = (VirtualThread) task.thread();
        enqueue(task, startCarrierFor(vt));
    }

    @Override
    public void onContinue(VirtualThreadTask task) {
        enqueue(task, continueCarrierFor((VirtualThread) task.thread()));
    }

    private void enqueue(VirtualThreadTask task, CarrierThread carrier) {
        VirtualThread vt = (VirtualThread) task.thread();
        if (!workStealingEnabled || !vt.isMpscStealable()) {
            vt.affinityHint = carrier.id;
            enqueueLocal(carrier, task);
            return;
        }
        if (carrier.queue.offerIfBelowThreshold(task, localStealableThreshold)) {
            vt.affinityHint = carrier.id;
            signalCarrier(carrier);
        } else {
            sharedQueue.offer(task);
            signalSharedWork();
        }
    }

    private static void enqueueLocal(CarrierThread carrier, VirtualThreadTask task) {
        carrier.queue.offer(task);
        signalCarrier(carrier);
    }

    private static void signalCarrier(CarrierThread carrier) {
        if (carrier.carrierState == CarrierThread.PARKED) {
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
    }

    private void signalSharedWork() {
        CarrierThread[] carriers = this.carriers;
        int start = Math.floorMod(nextProbe(), carriers.length);
        for (int i = 0; i < carriers.length; i++) {
            CarrierThread carrier = carriers[(start + i) % carriers.length];
            if (carrier.carrierState == CarrierThread.PARKED) {
                signalCarrier(carrier);
                return;
            }
        }
    }

    private CarrierThread startCarrierFor(VirtualThread vt) {
        int hint = vt.affinityHint;
        if (hint >= 0) {
            return carriers[Math.floorMod(hint, carriers.length)];
        }
        return carrierFor();
    }

    private CarrierThread continueCarrierFor(VirtualThread vt) {
        Thread caller = Thread.currentCarrierThread();
        if (caller instanceof CarrierThread ct && ct.scheduler == this) {
            return ct;
        }
        int hint = vt.affinityHint;
        if (hint >= 0) {
            return carriers[Math.floorMod(hint, carriers.length)];
        }
        return carrierFor();
    }

    private CarrierThread carrierFor() {
        Thread caller = Thread.currentCarrierThread();
        if (caller instanceof CarrierThread ct && ct.scheduler == this) {
            return ct;
        }
        return carriers[Math.floorMod(probe(), carriers.length)];
    }

    private static int probe() {
        int p = U.getInt(Thread.currentThread(), PROBE);
        if (p == 0) {
            long tid = Thread.currentThread().threadId();
            p = (int) (tid ^ (tid >>> 16));
            if (p == 0) p = 1;
            U.putInt(Thread.currentThread(), PROBE, p);
        }
        return p;
    }

    private static int nextProbe() {
        int p = probe();
        p ^= p << 13;
        p ^= p >>> 17;
        p ^= p << 5;
        if (p == 0) {
            p = 1;
        }
        U.putInt(Thread.currentThread(), PROBE, p);
        return p;
    }

    private VirtualThreadTask pollSharedTask(CarrierThread carrier) {
        if (!workStealingEnabled) {
            return null;
        }
        VirtualThreadTask task = sharedQueue.poll();
        if (task != null) {
            ((VirtualThread) task.thread()).affinityHint = carrier.id;
        }
        return task;
    }

    // ---- Carrier thread ----

    static final class CarrierThread extends Thread {
        static final int RUNNING = 0;
        static final int PARKED  = 1;

        final int id;
        final MpscUnboundedQueue<VirtualThreadTask> queue = new MpscUnboundedQueue<>(64);
        final MpscVirtualThreadScheduler scheduler;

        // carrier-local poller (Mode 4), null if using Mode 3 or lower
        CarrierLocalPoller poller;

        @Contended
        volatile int carrierState;

        CarrierThread(int id, MpscVirtualThreadScheduler scheduler) {
            super(null, null, "mpsc-carrier-" + id, 0, false);
            this.id = id;
            this.scheduler = scheduler;
            setDaemon(true);
        }

        @Override
        public void run() {
            if ("4".equals(System.getProperty("jdk.pollerMode"))) {
                try {
                    this.poller = new CarrierLocalPoller();
                    eventLoop();
                    return;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            plainLoop();
        }

        /**
         * Mode 4: interleave task draining with I/O polling.
         * Like Netty's EventLoop: drain tasks → poll I/O → drain → ...
         * Block in epoll_wait only when both queue and I/O are idle.
         */
        private static final long DRAIN_BUDGET_NS = java.util.concurrent.TimeUnit.MICROSECONDS
                .toNanos(Integer.getInteger("jdk.virtualThreadScheduler.drainBudgetUs", 50));
        private static final int TIME_CHECK_INTERVAL = 4;

        private void eventLoop() {
            var queue = this.queue;
            var poller = this.poller;
            int sharedPollCountdown = SHARED_POLL_INTERVAL;
            for (;;) {
                // drain tasks with time budget
                int drained = 0;
                long drainStart = System.nanoTime();
                VirtualThreadTask task;
                while ((task = queue.poll()) != null) {
                    runTask(task);
                    drained++;
                    if (scheduler.workStealingEnabled && --sharedPollCountdown == 0) {
                        sharedPollCountdown = SHARED_POLL_INTERVAL;
                        VirtualThreadTask sharedTask = scheduler.pollSharedTask(this);
                        if (sharedTask != null) {
                            runTask(sharedTask);
                            drained++;
                        }
                    }
                    if ((drained & (TIME_CHECK_INTERVAL - 1)) == 0
                            && System.nanoTime() - drainStart >= DRAIN_BUDGET_NS) {
                        break;
                    }
                }

                // non-blocking I/O poll
                int ioEvents = 0;
                try {
                    ioEvents = poller.poll(0);
                } catch (IOException e) { }

                if (drained + ioEvents > 0) {
                    continue;
                }

                if ((task = scheduler.pollSharedTask(this)) != null) {
                    sharedPollCountdown = SHARED_POLL_INTERVAL;
                    runTask(task);
                    continue;
                }

                // one more non-blocking check before parking
                try {
                    if (poller.poll(0) > 0) continue;
                } catch (IOException e) { }

                // Publish idleness before the final local/shared recheck so a concurrent
                // shared submit either wakes this carrier or is observed below before
                // the blocking poll. A missed immediate wake-up is therefore benign.
                carrierState = PARKED;

                if ((task = queue.poll()) != null) {
                    carrierState = RUNNING;
                    runTask(task);
                    continue;
                }
                if ((task = scheduler.pollSharedTask(this)) != null) {
                    carrierState = RUNNING;
                    sharedPollCountdown = SHARED_POLL_INTERVAL;
                    runTask(task);
                    continue;
                }

                try {
                    poller.poll(-1);
                } catch (IOException e) { }
                carrierState = RUNNING;
            }
        }

        /**
         * Plain loop (Mode 3 or lower): poll tasks, park when idle.
         */
        private void plainLoop() {
            var queue = this.queue;
            int sharedPollCountdown = SHARED_POLL_INTERVAL;
            for (;;) {
                VirtualThreadTask task = queue.poll();
                if (task != null) {
                    runTask(task);
                    if (scheduler.workStealingEnabled && --sharedPollCountdown == 0) {
                        sharedPollCountdown = SHARED_POLL_INTERVAL;
                        VirtualThreadTask sharedTask = scheduler.pollSharedTask(this);
                        if (sharedTask != null) {
                            runTask(sharedTask);
                        }
                    }
                    continue;
                }

                if ((task = scheduler.pollSharedTask(this)) != null) {
                    sharedPollCountdown = SHARED_POLL_INTERVAL;
                    runTask(task);
                    continue;
                }

                carrierState = PARKED;

                if ((task = queue.poll()) != null) {
                    carrierState = RUNNING;
                    runTask(task);
                    continue;
                }
                if ((task = scheduler.pollSharedTask(this)) != null) {
                    carrierState = RUNNING;
                    sharedPollCountdown = SHARED_POLL_INTERVAL;
                    runTask(task);
                    continue;
                }

                LockSupport.park();
                carrierState = RUNNING;
            }
        }

        private static void runTask(VirtualThreadTask task) {
            try {
                task.run();
            } catch (Throwable t) { }
        }
    }

    @Override
    public String toString() {
        return "MpscVirtualThreadScheduler[carriers=" + carriers.length
                + ",workStealing=" + workStealingEnabled + "]";
    }
}
