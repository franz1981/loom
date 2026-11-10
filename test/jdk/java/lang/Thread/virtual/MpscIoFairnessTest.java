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
 * @summary On the MPSC scheduler, I/O readiness reaches a reader on a carrier
 *          whose queue never empties: behind a lone yielder and behind an
 *          endless wake chain. Where a woken thread is queued: with stealing, on
 *          the carrier the wake was made on, else on its home
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
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
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscIoFairnessTest
 */

/*
 * @test id=mode4-one-carrier-no-interval
 * @summary The poll interval defeated: only the tests that need no interval poll run
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @run junit/othervm -esa
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
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.mpscMaxPollIntervalMicros=3600000000
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscIoFairnessTest
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscIoFairnessTest {
    // a hang fails by this limit, well inside the jtreg timeout
    private static final Duration LIMIT = Duration.ofSeconds(10);
    private static final boolean STEAL =
            Boolean.parseBoolean(
                    System.getProperty("jdk.virtualThreadScheduler.mpscSteal", "true"));
    private static final int PARALLELISM =
            Integer.getInteger("jdk.virtualThreadScheduler.parallelism");
    // set by the ids that take the interval out of the way of another poll rule
    private static final Long POLL_INTERVAL_MICROS =
            Long.getLong("jdk.virtualThreadScheduler.mpscMaxPollIntervalMicros");

    /** For tests that a carrier passes only by polling when the interval expires. */
    private static void assumeDefaultPollInterval() {
        assumeTrue(POLL_INTERVAL_MICROS == null, "needs the default poll interval");
    }

    @BeforeAll
    static void assertionsEnabled() throws Exception {
        Class<?> c = Class.forName("java.lang.MpscVirtualThreadScheduler");
        assertTrue(c.desiredAssertionStatus(), "run with -esa");
    }

    /** A connected loopback pair: a virtual thread reads, a platform thread writes. */
    private record SocketPair(Socket readerSide, Socket writerSide) implements AutoCloseable {
        static SocketPair open() throws IOException {
            try (ServerSocket listener = new ServerSocket()) {
                InetAddress lb = InetAddress.getLoopbackAddress();
                listener.bind(new InetSocketAddress(lb, 0));
                Socket writerSide = new Socket(lb, listener.getLocalPort());
                return new SocketPair(listener.accept(), writerSide);
            }
        }
        InputStream in() throws IOException {
            return readerSide.getInputStream();
        }
        OutputStream out() throws IOException {
            return writerSide.getOutputStream();
        }
        @Override
        public void close() throws IOException {
            try (readerSide; writerSide) { }
        }
    }

    /**
     * Starts the threads from one virtual thread, in order: a thread started by a
     * virtual thread starts on the starter's carrier.
     */
    private static void startTogether(List<Thread> threads) throws InterruptedException {
        Thread.ofVirtual().start(() -> threads.forEach(Thread::start)).join();
    }

    /** Waits until the thread is parked. */
    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + LIMIT.toNanos();
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING) {
            assertTrue(System.nanoTime() < deadline, thread + " did not park");
            Thread.sleep(1);
        }
    }

    /** Waits until the thread is parked in a socket read, waiting for data. */
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

    private static Thread currentCarrierThread() {
        try {
            Method m = Thread.class.getDeclaredMethod("currentCarrierThread");
            m.setAccessible(true);
            return (Thread) m.invoke(null);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(e);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch, String what) throws InterruptedException {
        assertTrue(latch.await(LIMIT.toMillis(), TimeUnit.MILLISECONDS), what + " within " + LIMIT);
    }

    /**
     * Wake chains, pairs of threads that park and unpark each other until stop is set,
     * two pairs per carrier. Each pair is started by its own platform thread, which
     * starts its virtual threads on one carrier, so consecutive starters cover every
     * carrier. Returns once chain threads have run on every carrier. A carrier with a
     * pair always has one of its threads queued, so it never runs dry, never steals,
     * and polls its poller only when the poll interval expires.
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
                        if (everyCarrier.getCount() > 0 && seen.add(currentCarrierThread())
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
     * A thread that yields in a loop, alone in its carrier's queue, does not keep a
     * reader started with it from receiving. With one carrier and the poll interval
     * defeated, only the poll before a lone yielder is requeued serves the reader.
     */
    @Test
    void testLoneYielderDoesNotStarveReader() throws Exception {
        try (SocketPair pair = SocketPair.open()) {
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
            startTogether(List.of(yielder, reader));
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
        try (SocketPair pair = SocketPair.open()) {
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
     * While every carrier runs an endless wake chain, a read with a timeout whose data
     * is written once the reader blocks returns the data well before the timeout: by
     * readiness, not by the timeout's wakeup.
     */
    @Test
    void testReadTimeoutNotSpurious() throws Exception {
        assumeDefaultPollInterval();
        int timeoutMillis = 2_000;
        try (SocketPair pair = SocketPair.open()) {
            pair.readerSide().setSoTimeout(timeoutMillis);
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger(-1);
            AtomicLong returnedAt = new AtomicLong();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            List<Thread> chains = wakeChains(stop);
            Thread reader = null;
            boolean served;
            long writtenAt;
            try {
                reader = Thread.ofVirtual().start(() -> {
                    try {
                        received.set(pair.in().read());
                        returnedAt.set(System.nanoTime());
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                awaitReadPark(reader);
                writtenAt = System.nanoTime();
                pair.out().write(42);
                served = reader.join(LIMIT);
            } finally {
                stopAll(stop, chains);
            }
            reader.join();
            rethrow(failure);
            assertTrue(served, "read did not return within " + LIMIT + " behind wake chains");
            assertEquals(42, received.get());
            long waitedMillis = TimeUnit.NANOSECONDS.toMillis(returnedAt.get() - writtenAt);
            assertTrue(waitedMillis < timeoutMillis / 2,
                    "read returned " + waitedMillis + " ms after the write, timeout " + timeoutMillis);
        }
    }

    /**
     * The carriers a thread X ran on: first, after an unpark by a virtual thread on
     * another carrier (the waker's carrier is recorded too), after a sleep, and after
     * a socket read. Every carrier runs yielders throughout, started from as many
     * platform threads, so that no carrier runs dry and steals.
     */
    private record Placement(Thread first, Thread waker, Thread afterUnpark,
                             Thread afterSleep, Thread afterRead) { }

    private static Placement runPlacement() throws Exception {
        try (SocketPair pair = SocketPair.open()) {
            Thread[] ranOn = new Thread[4];
            AtomicBoolean woken = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch unparked = new CountDownLatch(1);
            CountDownLatch reading = new CountDownLatch(1);
            Thread x = Thread.ofVirtual().start(() -> {
                try {
                    ranOn[0] = currentCarrierThread();
                    started.countDown();
                    while (!woken.get()) {
                        LockSupport.park();
                    }
                    ranOn[1] = currentCarrierThread();
                    unparked.countDown();
                    Thread.sleep(20);
                    ranOn[2] = currentCarrierThread();
                    reading.countDown();
                    int b = pair.in().read();
                    ranOn[3] = currentCarrierThread();
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
                awaitWaiting(x);
                assertFalse(parkedInRead(x), "X is parked in LockSupport.park, not in a read");
                Thread home = ranOn[0];
                Runnable yielder = () -> {
                    while (!stop.get()) {
                        Thread c = currentCarrierThread();
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
                // a platform thread starts its virtual threads on one carrier: one per
                // starter, so that consecutive starters cover every carrier
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
     * An unpark by a virtual thread on another carrier queues the thread on the
     * waker's carrier with stealing, on its home otherwise.
     */
    @Test
    void testVtWakePlacement() throws Exception {
        assumeDefaultPollInterval();
        Placement p = runPlacement();
        if (STEAL) {
            assertSame(p.waker(), p.afterUnpark(), p.toString());
        } else {
            assertSame(p.first(), p.afterUnpark(), p.toString());
        }
    }

    /**
     * A timer's wake goes to the carrier the thread last ran on.
     */
    @Test
    void testTimerWakeGoesToLastCarrier() throws Exception {
        assumeDefaultPollInterval();
        Placement p = runPlacement();
        assertSame(p.afterUnpark(), p.afterSleep(), p.toString());
        if (STEAL) {
            assertSame(p.waker(), p.afterSleep(), p.toString());
        } else {
            assertSame(p.first(), p.afterSleep(), p.toString());
        }
    }

    /**
     * Readiness is delivered; without stealing, or with one carrier, the thread runs
     * on the carrier it read on, where a thief cannot take it.
     */
    @Test
    void testReadinessStaysOnPollingCarrier() throws Exception {
        assumeDefaultPollInterval();
        Placement p = runPlacement();
        if (!STEAL || PARALLELISM == 1) {
            assertSame(p.afterSleep(), p.afterRead(), p.toString());
        }
    }
}
