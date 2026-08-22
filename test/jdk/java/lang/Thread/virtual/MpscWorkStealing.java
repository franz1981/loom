/*
 * Copyright (c) 2026, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
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

/*
 * @test id=default
 * @summary Test the opt-in work-stealing mode for the experimental MPSC scheduler
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm
 *     -Djdk.virtualThreadScheduler.useMpsc=true
 *     -Djdk.virtualThreadScheduler.parallelism=1
 *     MpscWorkStealing
 */

/*
 * @test id=single-carrier
 * @summary Test shared spillover and local-only affinity in the experimental MPSC scheduler
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm
 *     -Djdk.virtualThreadScheduler.useMpsc=true
 *     -Djdk.virtualThreadScheduler.parallelism=1
 *     -Djdk.virtualThreadScheduler.mpsc.workStealing=true
 *     -Djdk.virtualThreadScheduler.mpsc.localStealableThreshold=0
 *     MpscWorkStealing
 */

/*
 * @test id=two-carrier
 * @summary Test idle-carrier wake-up for the experimental MPSC work-stealing scheduler
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm
 *     -Djdk.virtualThreadScheduler.useMpsc=true
 *     -Djdk.virtualThreadScheduler.parallelism=2
 *     -Djdk.virtualThreadScheduler.mpsc.workStealing=true
 *     -Djdk.virtualThreadScheduler.mpsc.localStealableThreshold=0
 *     MpscWorkStealing
 */

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import jdk.test.lib.thread.VThreadScheduler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class MpscWorkStealing {
    private static final long TIMEOUT_MILLIS = 5_000L;
    private static final Thread.VirtualThreadScheduler SCHEDULER = VThreadScheduler.defaultScheduler();

    @Test
    void testWorkStealingDisabledByDefault() throws Exception {
        assumeFalse(isWorkStealingEnabled());
        assertNull(sharedQueue());
    }

    @Test
    void testStealableSpillsToSharedWhenLocalThresholdReached() throws Exception {
        assumeTrue(isSingleCarrierWorkStealingRun());

        var release = new AtomicBoolean();
        Thread blocker = Thread.ofVirtual()
                .stickyAffinity()
                .start(() -> busySpin(release));
        try {
            waitUntil(() -> currentCarrier(blocker) != null, "blocker to mount");

            Thread stealable = Thread.ofVirtual().start(() -> { });
            try {
                waitUntil(() -> sharedQueue().size() == 1, "shared spill queue to receive stealable task");
                assertEquals(0, localQueueSize(0));
            } finally {
                release.set(true);
                joinEventually(blocker, "blocker");
                joinEventually(stealable, "stealable");
            }
        } catch (Throwable t) {
            release.set(true);
            joinEventually(blocker, "blocker");
            throw t;
        }
    }

    @Test
    void testStickyAndRoundRobinStayOutOfSharedQueue() throws Exception {
        assumeTrue(isSingleCarrierWorkStealingRun());

        assertNonStealableStaysLocal(Thread.ofVirtual().stickyAffinity()::start);

        Thread builderRoundRobin = Thread.ofVirtual()
                .roundRobinAffinity()
                .unstarted(() -> { });
        assertTrue((int) getField(builderRoundRobin, "affinityHint") >= 0);

        ThreadFactory roundRobinFactory = Thread.ofVirtual()
                .roundRobinAffinity()
                .factory();
        assertNonStealableStaysLocal(task -> {
            Thread thread = roundRobinFactory.newThread(task);
            thread.start();
            return thread;
        });
    }

    @Test
    void testSharedQueuePolledWhileLocalWorkKeepsRunning() throws Exception {
        assumeTrue(isSingleCarrierWorkStealingRun());

        var entered = new AtomicBoolean();
        var sharedRan = new AtomicBoolean();
        Thread sticky = Thread.ofVirtual()
                .stickyAffinity()
                .start(() -> {
                    entered.set(true);
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
                    while (!sharedRan.get() && System.nanoTime() < deadline) {
                        Thread.yield();
                    }
                    assertTrue(sharedRan.get(), "shared task did not run while local work stayed runnable");
                });
        waitUntil(entered::get, "sticky runner to start");

        Thread shared = Thread.ofVirtual().start(() -> sharedRan.set(true));

        shared.join(TIMEOUT_MILLIS);
        sticky.join(TIMEOUT_MILLIS);
        assertFalse(shared.isAlive(), "shared task did not complete");
        assertFalse(sticky.isAlive(), "sticky task did not complete");
    }

    @Test
    void testSharedOfferWakesIdleCarrier() throws Exception {
        assumeTrue(isTwoCarrierWorkStealingRun());

        waitUntil(this::allCarriersParked, "all carriers to park");

        var ran = new AtomicBoolean();
        Thread thread = Thread.ofVirtual().start(() -> ran.set(true));
        thread.join(TIMEOUT_MILLIS);
        assertFalse(thread.isAlive(), "stealable task did not make progress");
        assertTrue(ran.get());
    }

    private static void assertNonStealableStaysLocal(ThreadStarter starter) throws Exception {
        var release = new AtomicBoolean();
        Thread blocker = Thread.ofVirtual()
                .stickyAffinity()
                .start(() -> busySpin(release));
        try {
            waitUntil(() -> currentCarrier(blocker) != null, "blocker to mount");

            Thread thread = starter.start(() -> { });
            try {
                waitUntil(() -> localQueueSize(0) == 1, "local carrier queue to receive non-stealable task");
                assertEquals(0, sharedQueue().size());
            } finally {
                release.set(true);
                joinEventually(blocker, "blocker");
                joinEventually(thread, "non-stealable");
            }
        } catch (Throwable t) {
            release.set(true);
            joinEventually(blocker, "blocker");
            throw t;
        }
    }

    private static boolean isWorkStealingEnabled() throws Exception {
        return (boolean) getField(SCHEDULER, "workStealingEnabled");
    }

    private static boolean isSingleCarrierWorkStealingRun() throws Exception {
        return isWorkStealingEnabled() && carriers().length == 1;
    }

    private static boolean isTwoCarrierWorkStealingRun() throws Exception {
        return isWorkStealingEnabled() && carriers().length == 2;
    }

    private static Object[] carriers() throws Exception {
        return (Object[]) getField(SCHEDULER, "carriers");
    }

    private static ConcurrentLinkedQueue<?> sharedQueue() throws Exception {
        return (ConcurrentLinkedQueue<?>) getField(SCHEDULER, "sharedQueue");
    }

    private static int localQueueSize(int index) throws Exception {
        Object queue = getField(carriers()[index], "queue");
        Method size = queue.getClass().getDeclaredMethod("size");
        size.setAccessible(true);
        return (int) size.invoke(queue);
    }

    private static Thread currentCarrier(Thread thread) throws Exception {
        return (Thread) getField(thread, "carrierThread");
    }

    private boolean allCarriersParked() {
        try {
            for (Object carrier : carriers()) {
                if ((int) getField(carrier, "carrierState") != 1) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Object getField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void busySpin(AtomicBoolean release) {
        while (!release.get()) {
            Thread.onSpinWait();
        }
    }

    private static void waitUntil(BooleanSupplier condition, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail("Timed out waiting for " + message);
            }
            Thread.sleep(10);
        }
    }

    private static void joinEventually(Thread thread, String message) throws InterruptedException {
        thread.join(TIMEOUT_MILLIS);
        assertFalse(thread.isAlive(), "Timed out waiting for " + message);
    }

    @FunctionalInterface
    private interface ThreadStarter {
        Thread start(Runnable task);
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean() throws Exception;
    }
}
