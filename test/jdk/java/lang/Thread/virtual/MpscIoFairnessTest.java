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
 * @test id=mode4
 * @summary I/O readiness reaches a reader on a carrier whose queue never empties
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode4-steal
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode4-one-carrier
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscIoFairnessTest
 */

/*
 * @test id=mode4-steal-one-carrier
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscIoFairnessTest
 */

/*
 * @test id=mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode2-steal
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode1
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode1-steal
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode4-one-carrier-no-interval
 * @summary The poll interval defeated: only the tests that need no interval poll run, and a
 *          poll that fills its event array is followed by another
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.mpscMaxPollIntervalMicros=3600000000
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscIoFairnessTest
 */

/*
 * @test id=mode4-steal-one-carrier-no-interval
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.mpscMaxPollIntervalMicros=3600000000
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscIoFairnessTest
 */

import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import jdk.test.lib.thread.VThreadPinner;
import jdk.test.lib.thread.VThreadScheduler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscIoFairnessTest {
    private static final Duration LIMIT = Duration.ofMillis(MpscTestSupport.DEADLINE_MS);
    private static final int PARALLELISM =
            Integer.getInteger("jdk.virtualThreadScheduler.parallelism");
    private static final int POLLER_MODE = Integer.getInteger("jdk.pollerMode");
    private static final Long POLL_INTERVAL_MICROS =
            Long.getLong("jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros");

    /** For tests that a carrier passes only by polling when the interval expires. */
    private static void assumeDefaultPollInterval() {
        assumeTrue(POLL_INTERVAL_MICROS == null, "needs the default poll interval");
    }

    @BeforeAll
    static void assertionsEnabled() throws Exception {
        MpscTestSupport.assertSchedulerAssertionsEnabled();
    }

    /** Waits until the thread is parked in a socket read. */
    private static void awaitReadPark(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + LIMIT.toNanos();
        while (!parkedInRead(thread)) {
            assertTrue(System.nanoTime() < deadline, thread + " did not park in a read");
            Thread.sleep(1);
        }
    }

    private static boolean parkedInRead(Thread thread) {
        Thread.State state = thread.getState();
        if (state != Thread.State.WAITING && state != Thread.State.TIMED_WAITING) {
            return false;
        }
        for (StackTraceElement e : thread.getStackTrace()) {
            if (e.getClassName().equals("sun.nio.ch.NioSocketImpl")
                    && e.getMethodName().equals("park")) {
                return true;
            }
        }
        return false;
    }

    private static void await(CountDownLatch latch, String what) throws InterruptedException {
        assertTrue(latch.await(LIMIT.toMillis(), TimeUnit.MILLISECONDS), what + " within " + LIMIT);
    }

    /**
     * Starts wake chains, pairs of threads that park and unpark each other until stop
     * is set, two pairs per carrier, and returns once chain threads have run on every
     * carrier. A carrier with a pair always has a thread queued: it never runs dry,
     * never steals, and polls only when the poll interval expires.
     */
    private static List<Thread> wakeChains(AtomicBoolean stop) throws InterruptedException {
        Set<Thread> seen = ConcurrentHashMap.newKeySet();
        CountDownLatch everyCarrier = new CountDownLatch(1);
        var threads = new ArrayList<Thread>();
        for (int i = 0; i < 2 * PARALLELISM; i++) {
            Thread[] chain = new Thread[2];
            for (int k = 0; k < 2; k++) {
                int other = 1 - k;
                chain[k] = Thread.ofVirtual().unstarted(() -> {
                    while (!stop.get()) {
                        if (everyCarrier.getCount() > 0
                                && seen.add(VThreadScheduler.currentCarrierThread())
                                && seen.size() == PARALLELISM) {
                            everyCarrier.countDown();
                        }
                        LockSupport.unpark(chain[other]);
                        LockSupport.park();
                    }
                    LockSupport.unpark(chain[other]);
                });
            }
            Thread.ofPlatform().start(() -> {
                chain[0].start();
                chain[1].start();
            }).join();
            threads.addAll(List.of(chain));
        }
        try {
            await(everyCarrier, "wake chains on every carrier");
        } catch (Throwable e) {
            stopAll(stop, threads);
            throw e;
        }
        return threads;
    }

    private static void stopAll(AtomicBoolean stop, List<Thread> threads)
            throws InterruptedException {
        stop.set(true);
        for (Thread t : threads) {
            t.join();
        }
    }

    private static void rethrow(AtomicReference<Throwable> failure) {
        if (failure.get() != null) {
            fail(failure.get());
        }
    }

    /**
     * A lone yielder in a carrier's queue does not keep a reader started with it from
     * receiving.
     */
    @Test
    void testLoneYielderDoesNotStarveReader() throws Exception {
        try (var pair = MpscTestSupport.SocketPair.open()) {
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger(-1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread yielder = Thread.ofVirtual().unstarted(() -> {
                while (!stop.get()) {
                    Thread.yield();
                }
            });
            Thread reader = Thread.ofVirtual().unstarted(() -> {
                try {
                    received.set(pair.in().read());
                } catch (Throwable e) {
                    failure.set(e);
                } finally {
                    stop.set(true);
                }
            });
            MpscTestSupport.startTogether(List.of(yielder, reader));
            awaitReadPark(reader);
            pair.out().write(42);
            boolean served = reader.join(LIMIT);
            stop.set(true);
            reader.join();
            yielder.join();
            rethrow(failure);
            assertTrue(served, "reader not served within " + LIMIT + " behind a lone yielder");
            assertEquals(42, received.get());
        }
    }

    /**
     * A reader receives every message while every carrier runs an endless wake chain.
     */
    @Test
    void testWakeChainDoesNotStarveReader() throws Exception {
        assumeDefaultPollInterval();
        int messages = 200;
        try (var pair = MpscTestSupport.SocketPair.open()) {
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> chains = wakeChains(stop);
            Thread reader = null;
            boolean served;
            int receivedInLimit;
            try {
                reader = Thread.ofVirtual().start(() -> {
                    try {
                        InputStream in = pair.in();
                        while (received.get() < messages && in.read() >= 0) {
                            received.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                awaitReadPark(reader);
                OutputStream out = pair.out();
                for (int i = 0; i < messages; i++) {
                    out.write(i);
                    out.flush();
                    Thread.sleep(1);
                }
                served = reader.join(LIMIT);
                receivedInLimit = received.get();
            } finally {
                stopAll(stop, chains);
            }
            reader.join();
            rethrow(failure);
            assertTrue(served, "reader received " + receivedInLimit + " of " + messages
                    + " within " + LIMIT + " behind wake chains");
            assertEquals(messages, received.get());
        }
    }

    /**
     * With every carrier running a wake chain, a timed read returns its data by
     * readiness; the timeout is twice the wait limit so it cannot serve the read.
     */
    @Test
    void testReadTimeoutNotSpurious() throws Exception {
        assumeDefaultPollInterval();
        try (var pair = MpscTestSupport.SocketPair.open()) {
            pair.readerSide().setSoTimeout((int) (2 * LIMIT.toMillis()));
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger(-1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> chains = wakeChains(stop);
            Thread reader = null;
            boolean served;
            try {
                reader = Thread.ofVirtual().start(() -> {
                    try {
                        received.set(pair.in().read());
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                awaitReadPark(reader);
                pair.out().write(42);
                served = reader.join(LIMIT);
            } finally {
                stopAll(stop, chains);
            }
            reader.join();
            rethrow(failure);
            assertTrue(served, "read did not return within " + LIMIT + " behind wake chains");
            assertEquals(42, received.get());
        }
    }

    /**
     * On the only carrier, held by a pinned thread while 200 fds become ready, so that its
     * first poll after the release fills the event array. Each served reader yields until
     * all are served: with 64 yielders queued, none polls for being alone, and the poll
     * interval never expires, so only the poll that follows a full one serves the rest.
     */
    @Test
    void testFullPollIsFollowedByAnother() throws Exception {
        assumeTrue(PARALLELISM == 1 && POLL_INTERVAL_MICROS != null
                && POLL_INTERVAL_MICROS >= 3_600_000_000L, "needs one carrier and no interval poll");
        int count = 200;
        var pairs = new ArrayList<MpscTestSupport.SocketPair>();
        var readers = new ArrayList<Thread>();
        AtomicBoolean stop = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch served = new CountDownLatch(count);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = null;
        long servedInTime;
        try {
            for (int i = 0; i < count; i++) {
                pairs.add(MpscTestSupport.SocketPair.open());
            }
            for (var pair : pairs) {
                readers.add(Thread.ofVirtual().start(() -> {
                    try {
                        assertEquals(42, pair.in().read());
                        served.countDown();
                        while (served.getCount() > 0 && !stop.get()) {
                            Thread.yield();
                        }
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                }));
            }
            for (Thread reader : readers) {
                awaitReadPark(reader);
            }
            holder = Thread.ofVirtual().start(() -> {
                try {
                    VThreadPinner.runPinned(release::await);
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            MpscTestSupport.awaitParked(holder);
            for (var pair : pairs) {
                pair.out().write(42);
            }
            release.countDown();
            served.await(LIMIT.toMillis(), TimeUnit.MILLISECONDS);
            servedInTime = count - served.getCount();
        } finally {
            release.countDown();
            stop.set(true);
            for (var pair : pairs) {
                pair.close();
            }
            for (Thread reader : readers) {
                reader.join();
            }
            if (holder != null) {
                holder.join();
            }
        }
        assertEquals(count, servedInTime, "readers served within " + LIMIT);
        rethrow(failure);
    }

    /**
     * Records the carriers a thread ran on: first, after an unpark by a virtual thread
     * on another carrier (the waker's too), after a sleep, and after a socket read.
     * Every carrier runs yielders throughout, so that none runs dry and steals.
     */
    private record Placement(Thread first, Thread waker, Thread afterUnpark,
                             Thread afterSleep, Thread afterRead) { }

    private static Placement runPlacement() throws Exception {
        try (var pair = MpscTestSupport.SocketPair.open()) {
            Thread[] ranOn = new Thread[4];
            AtomicBoolean woken = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch unparked = new CountDownLatch(1);
            CountDownLatch reading = new CountDownLatch(1);
            Thread x = Thread.ofVirtual().start(() -> {
                try {
                    ranOn[0] = VThreadScheduler.currentCarrierThread();
                    started.countDown();
                    while (!woken.get()) {
                        LockSupport.park();
                    }
                    ranOn[1] = VThreadScheduler.currentCarrierThread();
                    unparked.countDown();
                    Thread.sleep(20);
                    ranOn[2] = VThreadScheduler.currentCarrierThread();
                    reading.countDown();
                    int b = pair.in().read();
                    ranOn[3] = VThreadScheduler.currentCarrierThread();
                    assertEquals(42, b);
                } catch (Throwable e) {
                    failure.set(e);
                }
            });

            AtomicBoolean stop = new AtomicBoolean();
            AtomicBoolean unparkRequested = new AtomicBoolean();
            AtomicBoolean claimed = new AtomicBoolean();
            AtomicReference<Thread> waker = new AtomicReference<>();
            Set<Thread> seen = ConcurrentHashMap.newKeySet();
            CountDownLatch allCarriersBusy = new CountDownLatch(1);
            var yielders = new ArrayList<Thread>();
            try {
                await(started, "X started");
                MpscTestSupport.awaitParked(x);
                assertFalse(parkedInRead(x), "X is parked in LockSupport.park, not in a read");
                Thread home = ranOn[0];
                Runnable yielder = () -> {
                    while (!stop.get()) {
                        Thread c = VThreadScheduler.currentCarrierThread();
                        if (seen.add(c) && seen.size() == PARALLELISM) {
                            allCarriersBusy.countDown();
                        }
                        if (unparkRequested.get() && (c != home || PARALLELISM == 1)
                                && claimed.compareAndSet(false, true)) {
                            waker.set(c);
                            woken.set(true);
                            LockSupport.unpark(x);
                        }
                        Thread.yield();
                    }
                };
                // one starter per carrier: a platform thread's virtual threads start on one carrier
                for (int i = 0; i < 2 * PARALLELISM; i++) {
                    var starter = Thread.ofPlatform().start(
                            () -> yielders.add(Thread.ofVirtual().start(yielder)));
                    starter.join();
                }
                await(allCarriersBusy, "yielders on every carrier");
                unparkRequested.set(true);
                await(unparked, "X ran after the unpark");
                await(reading, "X ran after the sleep");
                awaitReadPark(x);
                pair.out().write(42);
                assertTrue(x.join(LIMIT), "X not served within " + LIMIT);
            } finally {
                stop.set(true);
                for (Thread t : yielders) {
                    t.join();
                }
                x.join();
            }
            rethrow(failure);
            return new Placement(ranOn[0], waker.get(), ranOn[1], ranOn[2], ranOn[3]);
        }
    }

    /**
     * Where a woken thread runs: after an unpark by a virtual thread on another
     * carrier, the waker's carrier with stealing, its home otherwise; after a sleep,
     * the carrier it last ran on; after a socket read, the carrier it read on. The read
     * is not asserted in Mode 2 with stealing on several carriers, where a read
     * sub-poller virtual thread makes the wake on whatever carrier it runs on.
     */
    @Test
    void testWakePlacement() throws Exception {
        assumeDefaultPollInterval();
        Placement p = runPlacement();
        assertSame(MpscTestSupport.STEAL ? p.waker() : p.first(), p.afterUnpark(), "unpark: " + p);
        assertSame(p.afterUnpark(), p.afterSleep(), "timer: " + p);
        if (!(MpscTestSupport.STEAL && PARALLELISM > 1 && POLLER_MODE == 2)) {
            assertSame(p.afterSleep(), p.afterRead(), "read: " + p);
        }
    }
}
