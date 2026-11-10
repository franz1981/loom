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
 * @summary Thread.yield and Thread.sleep(0) on the MPSC scheduler, with the
 *          scheduler's assertions enabled: yielders started together and apart,
 *          mixed with parking, I/O readiness and pinned yields
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
 */

/*
 * @test id=mode4-steal
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
 */

/*
 * @test id=mode4-one-carrier
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscYieldTest
 */

/*
 * @test id=mode4-steal-one-carrier
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscYieldTest
 */

/*
 * @test id=mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
 */

/*
 * @test id=mode2-steal
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
 */

/*
 * @test id=mode1
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
 */

/*
 * @test id=mode1-steal
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=4 MpscYieldTest
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import jdk.test.lib.thread.VThreadPinner;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MpscYieldTest {
    private static final int YIELDS = 2_000;
    private static final boolean STEAL =
            Boolean.parseBoolean(
                    System.getProperty("jdk.virtualThreadScheduler.mpscSteal", "true"));

    @BeforeAll
    static void assertionsEnabled() throws Exception {
        // the scheduler's own asserts are what this test exercises
        Class<?> c = Class.forName("java.lang.MpscVirtualThreadScheduler");
        assertTrue(c.desiredAssertionStatus(), "run with -esa");
    }

    private static void yieldOnce(int i) {
        if ((i & 1) == 0) {
            Thread.yield();
        } else {
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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

    /**
     * Starts the threads from one virtual thread: a thread started by a virtual
     * thread starts on the starter's carrier, and without stealing stays there.
     */
    private static void startTogether(List<Thread> threads) throws InterruptedException {
        Thread.ofVirtual().start(() -> threads.forEach(Thread::start)).join();
    }

    private static void joinAll(List<Thread> threads) throws InterruptedException {
        for (Thread t : threads) {
            t.join();
        }
    }

    /**
     * Yielders started together complete and each yield returns; without stealing
     * they all run on the carrier they started on.
     */
    @Test
    void testYieldersStartedTogether() throws Exception {
        Set<Thread> carriers = ConcurrentHashMap.newKeySet();
        AtomicInteger yields = new AtomicInteger();
        var threads = new ArrayList<Thread>();
        for (int k = 0; k < 8; k++) {
            threads.add(Thread.ofVirtual().unstarted(() -> {
                for (int i = 0; i < YIELDS; i++) {
                    carriers.add(currentCarrierThread());
                    yieldOnce(i);
                    yields.incrementAndGet();
                }
            }));
        }
        startTogether(threads);
        joinAll(threads);
        assertEquals(8 * YIELDS, yields.get());
        if (!STEAL) {
            assertEquals(1, carriers.size(), carriers.toString());
        }
    }

    /**
     * Yielders started from a platform thread, more than there are carriers,
     * complete; a lone yielder too.
     */
    @Test
    void testYieldersStartedApart() throws Exception {
        AtomicInteger yields = new AtomicInteger();
        var threads = new ArrayList<Thread>();
        for (int k = 0; k < 16; k++) {
            threads.add(Thread.ofVirtual().start(() -> {
                for (int i = 0; i < YIELDS; i++) {
                    yieldOnce(i);
                    yields.incrementAndGet();
                }
            }));
        }
        joinAll(threads);
        assertEquals(16 * YIELDS, yields.get());

        Thread lone = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < YIELDS; i++) {
                yieldOnce(i);
            }
        });
        lone.join();
    }

    /**
     * Yielders, half started together and half from a platform thread, that also
     * park and unpark one another, so that yields, park races and wakeups from other
     * carriers interleave.
     */
    @Test
    void testYieldAndPark() throws Exception {
        int n = 6;
        Thread[] ring = new Thread[n];
        AtomicInteger done = new AtomicInteger();
        AtomicBoolean stop = new AtomicBoolean();
        for (int k = 0; k < n; k++) {
            int me = k;
            Runnable task = () -> {
                for (int i = 0; i < YIELDS; i++) {
                    yieldOnce(i);
                    LockSupport.unpark(ring[(me + 1) % n]);
                    if ((i % 7) == 0) {
                        LockSupport.parkNanos(10_000);
                    }
                }
                done.incrementAndGet();
            };
            ring[k] = Thread.ofVirtual().unstarted(task);
        }
        var together = new ArrayList<Thread>();
        for (int k = 0; k < n; k++) {
            if (k % 2 == 0) {
                together.add(ring[k]);
            } else {
                ring[k].start();
            }
        }
        startTogether(together);
        // a platform thread unparks them too, from outside every carrier
        Thread unparker = Thread.ofPlatform().start(() -> {
            while (!stop.get()) {
                for (Thread t : ring) {
                    LockSupport.unpark(t);
                }
                Thread.onSpinWait();
            }
        });
        try {
            for (Thread t : ring) {
                t.join();
            }
        } finally {
            stop.set(true);
            unparker.join();
        }
        assertEquals(n, done.get());
    }

    /**
     * A reader started together with yielders receives every message written from a
     * platform thread: without stealing, or with one carrier, I/O readiness and
     * yields interleave on one carrier.
     */
    @Test
    void testReaderBehindYielders() throws Exception {
        int messages = 200;
        try (ServerSocket listener = new ServerSocket()) {
            InetAddress lb = InetAddress.getLoopbackAddress();
            listener.bind(new InetSocketAddress(lb, 0));
            try (Socket writerSide = new Socket(lb, listener.getLocalPort());
                 Socket readerSide = listener.accept()) {
                AtomicBoolean stop = new AtomicBoolean();
                AtomicInteger received = new AtomicInteger();
                var yielders = new ArrayList<Thread>();
                for (int k = 0; k < 4; k++) {
                    yielders.add(Thread.ofVirtual().unstarted(() -> {
                        int i = 0;
                        while (!stop.get()) {
                            yieldOnce(i++);
                        }
                    }));
                }
                Thread reader = Thread.ofVirtual().unstarted(() -> {
                    try {
                        InputStream in = readerSide.getInputStream();
                        while (received.get() < messages) {
                            if (in.read() < 0) {
                                break;
                            }
                            received.incrementAndGet();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                var together = new ArrayList<Thread>(yielders);
                together.add(reader);
                startTogether(together);
                OutputStream out = writerSide.getOutputStream();
                for (int i = 0; i < messages; i++) {
                    out.write(i);
                    out.flush();
                    Thread.sleep(1);
                }
                reader.join();
                stop.set(true);
                joinAll(yielders);
                assertEquals(messages, received.get());
            }
        }
    }

    /**
     * A yield while pinned (a native frame on the stack) does not unmount and
     * returns; a yield while holding a monitor, and the yields around both, return.
     */
    @Test
    void testPinnedYield() throws Exception {
        Object lock = new Object();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    int n = i;
                    Thread before = currentCarrierThread();
                    VThreadPinner.runPinned(() -> yieldOnce(n));
                    assertSame(before, currentCarrierThread());
                    synchronized (lock) {
                        yieldOnce(i);
                    }
                    yieldOnce(i);
                }
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        t.join();
        if (failure.get() != null) {
            fail(failure.get());
        }
    }
}
