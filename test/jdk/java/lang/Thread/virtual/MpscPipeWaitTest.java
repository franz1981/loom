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
 * @test id=mpsc
 * @summary MPSC scheduler, Mode 4: a subprocess pipe read or write that would wait parks on a
 *          poller and uses no pool thread
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17 MpscPipeWaitTest
 */

/*
 * @test id=mpsc-p4
 * @summary Parallelism 4: readers that wait on one carrier's poller and resume on another
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=4
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=20 MpscPipeWaitTest
 */

/*
 * @test id=mpsc-nopool
 * @summary No pool thread allowed: the waits still leave the only carrier free
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=1 MpscPipeWaitTest
 */

/*
 * @test id=mpsc-mode2
 * @summary Mode 2: no local poller, the waits move to the pool as before; the outcomes hold
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17 MpscPipeWaitTest
 */

/*
 * @test id=fjp
 * @summary ForkJoinPool, the control
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=false
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscPipeWaitTest
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jdk.test.lib.thread.VThreadPinner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscPipeWaitTest {
    private static final boolean MPSC = MpscTestSupport.MPSC;
    private static final boolean MODE4 = MPSC && System.getProperty("jdk.pollerMode", "4").equals("4");
    private static final int PARALLELISM = MpscTestSupport.PARALLELISM;
    private static final int CAP = MpscTestSupport.POOL_CAP;
    private static final long DEADLINE_MS = MpscTestSupport.DEADLINE_MS;
    // more than a pipe holds: 16 pages, 64 KiB with 4 KiB pages, 1 MiB with 64 KiB pages
    private static final int MORE_THAN_A_PIPE = 2 * 1024 * 1024;

    private final List<Process> children = new ArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        for (Process p : children) {
            p.destroyForcibly().waitFor();
        }
        children.clear();
    }

    private Process start(String... cmd) throws IOException {
        Process p = new ProcessBuilder(cmd).start();
        children.add(p);
        return p;
    }

    /** A virtual thread that reads up to 100 bytes once, recording the count or the exception. */
    private static final class Reader {
        final Thread thread;
        final AtomicInteger n = new AtomicInteger(-2);
        final byte[] buf = new byte[100];
        final AtomicReference<Throwable> thrown = new AtomicReference<>();
        final AtomicBoolean interruptedAfter = new AtomicBoolean();

        Reader(InputStream in, boolean pinned) {
            thread = Thread.ofVirtual().start(() -> {
                try {
                    if (pinned) {
                        VThreadPinner.runPinned(() -> n.set(in.read(buf, 0, buf.length)));
                    } else {
                        n.set(in.read(buf, 0, buf.length));
                    }
                } catch (Throwable e) {
                    thrown.set(e);
                }
                interruptedAfter.set(Thread.currentThread().isInterrupted());
            });
        }

        void awaitWaiting() throws InterruptedException {
            MpscPipeWaitTest.awaitWaiting(thread);
        }

        void join(String what) throws InterruptedException {
            assertTrue(thread.join(Duration.ofMillis(DEADLINE_MS)), what + ": did not return");
        }
    }

    // ---- reads ----

    /**
     * A waiting read of a child's stdout frees the only carrier, uses no pool thread in Mode 4, and
     * returns the later bytes.
     */
    @Test
    void waitingReadFreesTheCarrier() throws Exception {
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        if (MODE4) {
            assertEquals(List.of(), MpscTestSupport.poolThreads(), "a waiting pipe read used a pool thread");
            MpscTestSupport.assertRunsNow("a new thread while a pipe read waits");
        } else if (CAP > 0 || !MPSC) {
            MpscTestSupport.assertRunsNow("a new thread while a pipe read waits");
        }
        OutputStream w = cat.getOutputStream();
        w.write("hi\n".getBytes());
        w.flush();
        r.join("reader");
        assertNull(r.thrown.get());
        assertEquals(3, r.n.get());
        assertEquals("hi\n", new String(r.buf, 0, 3));
    }

    /** A read that waited returns what is there when it wakes: fewer bytes than asked. */
    @Test
    void partialRead() throws Exception {
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        OutputStream w = cat.getOutputStream();
        w.write(new byte[] { 'a', 'b', 'c', 'd', 'e' });
        w.flush();
        r.join("reader");
        assertNull(r.thrown.get());
        assertTrue(r.n.get() >= 1 && r.n.get() <= 5, "read " + r.n.get());
    }

    /** The child exits without writing: the waiting read returns the end of stream. */
    @Test
    void endOfStream() throws Exception {
        Process p = start("sh", "-c", "read line");
        Reader r = new Reader(p.getInputStream(), false);
        r.awaitWaiting();
        try (OutputStream w = p.getOutputStream()) {
            w.write('\n');
        }
        r.join("reader at the end of stream");
        assertNull(r.thrown.get());
        assertEquals(-1, r.n.get());
    }

    /**
     * Closing the stream during a waiting read: when the child then writes, the read throws "Stream
     * closed".
     */
    @Test
    void closeDoesNotEndAWaitingRead() throws Exception {
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        cat.getInputStream().close();
        OutputStream w = cat.getOutputStream();
        w.write('c');
        w.flush();
        r.join("reader after the child wrote");
        assertInstanceOf(IOException.class, r.thrown.get(), "read after close: " + r.n.get());
        assertTrue(cat.isAlive());
    }

    /**
     * A close during a waiting read leaves the fd open until the read returns (checked in
     * /proc/self/fd).
     */
    @Test
    void aDeferredCloseClosesTheFd() throws Exception {
        Map<String, String> before = pipeFds();
        Process cat = new ProcessBuilder("cat").redirectErrorStream(true).start();
        children.add(cat);
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        Set<String> made = newPipeFds(before);
        assertEquals(2, made.size(), "pipes of the process start (stdin, stdout): " + made);
        cat.getInputStream().close();
        // close returns after it closed the fd or deferred the close
        if (MODE4) {
            assertEquals(made, newPipeFds(before), "the fd was closed while a read waited on it");
        }
        OutputStream w = cat.getOutputStream();
        w.write('x');
        w.flush();
        r.join("reader after the child wrote");
        assertInstanceOf(IOException.class, r.thrown.get(), "read after close: " + r.n.get());
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        while (newPipeFds(before).size() != 1) {
            assertTrue(System.nanoTime() < end, "the stream's fd was not closed: " + newPipeFds(before));
            Thread.sleep(5);
        }
    }

    /**
     * Parallelism above 1: 8 readers make 200 round trips each through a child cat; every byte
     * comes back, no pool thread is used.
     */
    @Test
    void readersThatChangeCarrier() throws Exception {
        assumeTrue(MODE4 && PARALLELISM > 1 && MpscTestSupport.STEAL, "Mode 4, parallelism above 1, stealing");
        int readers = 8;
        List<Thread> ts = new ArrayList<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        AtomicInteger moved = new AtomicInteger();
        for (int i = 0; i < readers; i++) {
            Process cat = start("cat");
            ts.add(Thread.ofVirtual().start(() -> {
                try {
                    InputStream in = cat.getInputStream();
                    OutputStream out = cat.getOutputStream();
                    byte[] b = new byte[1];
                    Set<String> carriers = new HashSet<>();
                    for (int k = 0; k < 200; k++) {
                        out.write(k);
                        out.flush();
                        carriers.add(MpscTestSupport.carrierName());
                        assertEquals(1, in.read(b, 0, 1));
                        assertEquals((byte) k, b[0]);
                        long until = System.nanoTime() + 100_000;
                        while (System.nanoTime() < until) {
                            Thread.onSpinWait();
                        }
                    }
                    if (carriers.size() > 1) {
                        moved.incrementAndGet();
                    }
                } catch (Throwable e) {
                    failed.set(e);
                }
            }));
        }
        for (Thread t : ts) {
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS * 4)), "a reader did not finish");
        }
        assertNull(failed.get(), () -> "a reader failed: " + failed.get());
        assumeTrue(moved.get() > 0, "no reader changed carrier: the test would prove nothing");
        assertEquals(List.of(), MpscTestSupport.poolThreads(), "a waiting pipe read used a pool thread");
    }

    /** destroy while a read waits: the read returns the end of stream, -1, as on ForkJoinPool. */
    @Test
    void destroyWhileAReadWaits() throws Exception {
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        cat.destroy();
        r.join("reader after destroy");
        assertNull(r.thrown.get(), "read after destroy threw");
        assertEquals(-1, r.n.get());
    }

    /**
     * An interrupt does not end a waiting pipe read: it returns the bytes written later, with the
     * interrupt status kept.
     */
    @Test
    void interruptDoesNotEndAWaitingRead() throws Exception {
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), false);
        r.awaitWaiting();
        r.thread.interrupt();
        OutputStream w = cat.getOutputStream();
        w.write('z');
        w.flush();
        r.join("reader");
        assertNull(r.thrown.get());
        assertEquals(1, r.n.get());
        assertTrue(r.interruptedAfter.get(), "interrupt status lost");
        if (MODE4) {
            assertEquals(List.of(), MpscTestSupport.poolThreads());
        }
    }

    /**
     * A reader interrupted before and while it waits still waits, returns the later byte and
     * keeps its interrupt status.
     */
    @Test
    void interruptedReaderStillWaits() throws Exception {
        Process cat = start("cat");
        InputStream in = cat.getInputStream();
        AtomicInteger n = new AtomicInteger(-2);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        AtomicBoolean interruptedAfter = new AtomicBoolean();
        Thread t = Thread.ofVirtual().start(() -> {
            Thread.currentThread().interrupt();
            try {
                n.set(in.read(new byte[10], 0, 10));
            } catch (Throwable e) {
                thrown.set(e);
            }
            interruptedAfter.set(Thread.currentThread().isInterrupted());
        });
        awaitWaiting(t);
        t.interrupt();
        awaitWaiting(t);
        t.interrupt();
        awaitWaiting(t);
        OutputStream w = cat.getOutputStream();
        w.write('z');
        w.flush();
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)), "the read did not return");
        assertNull(thrown.get());
        assertEquals(1, n.get());
        assertTrue(interruptedAfter.get(), "interrupt status lost");
    }

    /** A pinned reader cannot unmount: its read still completes when the bytes come. */
    @Test
    void pinnedRead() throws Exception {
        assumeTrue(CAP > 0 || MODE4 || !MPSC, "a pinned read with no pool blocks the only carrier");
        Process cat = start("cat");
        Reader r = new Reader(cat.getInputStream(), true);
        // pinned: it cannot park on a poller, and blocks in read(2) or parks pinned
        MpscTestSupport.awaitParkedOrIn(r.thread, "readBytes");
        OutputStream w = cat.getOutputStream();
        w.write('p');
        w.flush();
        r.join("pinned reader");
        assertNull(r.thrown.get());
        assertEquals(1, r.n.get());
    }

    // ---- writes ----

    /**
     * A write of more than a pipe holds waits for room without holding the carrier, and every byte
     * arrives.
     */
    @Test
    void writeWaitsForRoom() throws Exception {
        Path go = MpscTestSupport.mkfifo();
        Process p = start("sh", "-c", "read line < \"$0\"; exec wc -c", go.toString());
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread writer = Thread.ofVirtual().start(() -> {
            try (OutputStream out = p.getOutputStream()) {
                out.write(new byte[MORE_THAN_A_PIPE]);
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        awaitWaitingWrite(writer);
        assertTrue(writer.isAlive(), "the write did not wait");
        if (MODE4) {
            assertEquals(List.of(), MpscTestSupport.poolThreads(), "a waiting pipe write used a pool thread");
            MpscTestSupport.assertRunsNow("a new thread while a pipe write waits");
        }
        Files.writeString(go, "go\n");   // the child starts reading
        assertTrue(writer.join(Duration.ofMillis(DEADLINE_MS)), "writer did not finish");
        assertNull(thrown.get());
        String count = new String(p.getInputStream().readAllBytes()).strip();
        assertEquals(String.valueOf(MORE_THAN_A_PIPE), count);
    }

    /**
     * destroy while a write waits for room: the writer wakes with an IOException and, in Mode 4,
     * uses no pool thread.
     */
    @Test
    void destroyWakesAWaitingWrite() throws Exception {
        Process p = start("sh", "-c", "exec sleep 3600");
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        OutputStream out = p.getOutputStream();
        Thread writer = Thread.ofVirtual().start(() -> {
            try {
                out.write(new byte[MORE_THAN_A_PIPE]);
                out.flush();
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        awaitWaitingWrite(writer);
        assertTrue(writer.isAlive(), "the write did not wait");
        p.destroy();
        assertTrue(writer.join(Duration.ofMillis(DEADLINE_MS)), "writer did not wake");
        assertInstanceOf(IOException.class, thrown.get());
        if (MODE4) {
            assertEquals(List.of(), MpscTestSupport.poolThreads(), "the broken-pipe write moved to a pool thread");
        }
    }

    // ---- helpers ----

    /** Waits until a read waits: parked on a poller in Mode 4, else in readBytes. */
    private static void awaitWaiting(Thread t) throws InterruptedException {
        if (MODE4) {
            MpscTestSupport.awaitParked(t);
        } else {
            MpscTestSupport.awaitParkedOrIn(t, "readBytes");
        }
    }

    /** As awaitWaiting, for a write: in Mode 4 parked on a write poller, else in writeBytes. */
    private static void awaitWaitingWrite(Thread t) throws InterruptedException {
        if (MODE4) {
            MpscTestSupport.awaitParked(t);
        } else {
            MpscTestSupport.awaitParkedOrIn(t, "writeBytes");
        }
    }

    /** fd number -> link target, for the pipes this process has open. */
    private static Map<String, String> pipeFds() throws IOException {
        Map<String, String> m = new HashMap<>();
        try (var fds = Files.list(Path.of("/proc/self/fd"))) {
            for (Path fd : fds.toList()) {
                try {
                    String target = Files.readSymbolicLink(fd).toString();
                    if (target.startsWith("pipe:")) {
                        m.put(fd.getFileName().toString(), target);
                    }
                } catch (IOException e) {
                    // closed meanwhile, or the listing's own fd
                }
            }
        }
        return m;
    }

    /** The pipe fds open now that were not open, on the same pipe, in before. */
    private static Set<String> newPipeFds(Map<String, String> before) throws IOException {
        Set<String> made = new HashSet<>();
        pipeFds().forEach((fd, target) -> {
            if (!target.equals(before.get(fd))) {
                made.add(fd + "->" + target);
            }
        });
        return made;
    }
}
