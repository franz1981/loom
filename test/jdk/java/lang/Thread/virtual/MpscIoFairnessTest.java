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
 *          endless wake chain
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @run junit/othervm -esa
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MpscIoFairnessTest {
    // a hang fails by this limit, well inside the jtreg timeout
    private static final Duration LIMIT = Duration.ofSeconds(10);

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

    /** Waits until the thread is parked, e.g. blocked in a read. */
    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + LIMIT.toNanos();
        while (thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING) {
            assertTrue(System.nanoTime() < deadline, thread + " did not park");
            Thread.sleep(1);
        }
    }

    /**
     * Two threads that park and unpark each other until stop is set, so that the
     * carrier they share always has one of them queued.
     */
    private static Thread[] wakeChain(AtomicBoolean stop) {
        Thread[] chain = new Thread[2];
        for (int k = 0; k < 2; k++) {
            int other = 1 - k;
            chain[k] = Thread.ofVirtual().unstarted(() -> {
                while (!stop.get()) {
                    LockSupport.unpark(chain[other]);
                    LockSupport.park();
                }
                LockSupport.unpark(chain[other]);
            });
        }
        return chain;
    }

    private static void rethrow(AtomicReference<Throwable> failure) {
        if (failure.get() != null) {
            fail(failure.get());
        }
    }

    /**
     * A thread that yields in a loop, alone in its carrier's queue, does not keep a
     * reader started with it from receiving.
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
            awaitWaiting(reader);
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
     * A reader on the carrier of an endless wake chain receives every message.
     */
    @Test
    void testWakeChainDoesNotStarveReader() throws Exception {
        int messages = 200;
        try (SocketPair pair = SocketPair.open()) {
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = Thread.ofVirtual().unstarted(() -> {
                try {
                    InputStream in = pair.in();
                    while (received.get() < messages && in.read() >= 0) {
                        received.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            Thread[] chain = wakeChain(stop);
            startTogether(List.of(reader, chain[0], chain[1]));
            awaitWaiting(reader);
            OutputStream out = pair.out();
            for (int i = 0; i < messages; i++) {
                out.write(i);
                out.flush();
                Thread.sleep(1);
            }
            boolean served = reader.join(LIMIT);
            int receivedInLimit = received.get();
            stop.set(true);
            for (Thread t : chain) {
                t.join();
            }
            reader.join();
            rethrow(failure);
            assertTrue(served, "reader received " + receivedInLimit + " of " + messages
                    + " within " + LIMIT + " behind a wake chain");
            assertEquals(messages, received.get());
        }
    }

    /**
     * On the carrier of an endless wake chain, a read with a timeout whose data is
     * written once the reader blocks returns the data well before the timeout: by
     * readiness, not by the timeout's wakeup.
     */
    @Test
    void testReadTimeoutNotSpurious() throws Exception {
        int timeoutMillis = 2_000;
        try (SocketPair pair = SocketPair.open()) {
            pair.readerSide().setSoTimeout(timeoutMillis);
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger received = new AtomicInteger(-1);
            AtomicLong returnedAt = new AtomicLong();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread reader = Thread.ofVirtual().unstarted(() -> {
                try {
                    received.set(pair.in().read());
                    returnedAt.set(System.nanoTime());
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            Thread[] chain = wakeChain(stop);
            startTogether(List.of(reader, chain[0], chain[1]));
            awaitWaiting(reader);
            long writtenAt = System.nanoTime();
            pair.out().write(42);
            boolean served = reader.join(LIMIT);
            stop.set(true);
            for (Thread t : chain) {
                t.join();
            }
            reader.join();
            rethrow(failure);
            assertTrue(served, "read did not return within " + LIMIT + " behind a wake chain");
            assertEquals(42, received.get());
            long waitedMillis = TimeUnit.NANOSECONDS.toMillis(returnedAt.get() - writtenAt);
            assertTrue(waitedMillis < timeoutMillis / 2,
                    "read returned " + waitedMillis + " ms after the write, timeout " + timeoutMillis);
        }
    }
}
