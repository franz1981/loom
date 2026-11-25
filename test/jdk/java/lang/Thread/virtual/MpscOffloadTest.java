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
 * @summary MPSC scheduler: a Blocker section that waits on another party moves its virtual thread
 *          to an offload pool thread
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadTest
 */

/*
 * @test id=mpsc-mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadTest
 */

/*
 * @test id=mpsc-steal-off
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadTest
 */

/*
 * @test id=mpsc-p2-cap8
 * @summary Two carriers and a pool bound of 8: a burst places every section, the cap holds
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=2
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=10
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadTest
 */

/*
 * @test id=mpsc-cap1
 * @summary The pool's bound: maxPoolSize - parallelism = 1
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=2
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadTest
 */

/*
 * @test id=mpsc-nopool
 * @summary maxPoolSize = parallelism: no pool thread, sections run on the carrier
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=1 MpscOffloadTest
 */

/*
 * @test id=fjp
 * @summary ForkJoinPool: the same operations, as a control (no pool thread exists)
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/java.lang:+open java.base/jdk.internal.misc java.base/sun.nio.ch
 *          java.management jdk.jfr
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=false
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscOffloadTest
 */

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jdk.internal.misc.Blocker;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import jdk.test.lib.process.ProcessTools;
import jdk.test.lib.thread.VThreadPinner;
import jdk.test.lib.thread.VThreadRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscOffloadTest {
    private static final boolean MPSC = MpscTestSupport.MPSC;
    private static final int CAP = MpscTestSupport.POOL_CAP;
    private static final long KEEP_ALIVE_MS = MpscTestSupport.KEEP_ALIVE_MS;
    private static final long DEADLINE_MS = MpscTestSupport.DEADLINE_MS;

    private static ServerSocket listener;
    private final List<AutoCloseable> closeables = new ArrayList<>();

    @BeforeAll
    static void listen() throws Exception {
        listener = new ServerSocket(0, 512, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> { }).join();
    }

    @AfterAll
    static void close() throws IOException {
        listener.close();
    }

    @AfterEach
    void cleanup() throws Exception {
        for (int i = closeables.size() - 1; i >= 0; i--) {
            closeables.get(i).close();
        }
        closeables.clear();
    }

    private static void assumePool() {
        assumeTrue(MPSC && CAP > 0, "needs an MPSC offload pool");
    }

    // ---- a waiting section does not hold the carrier ----

    /** p=1: while a section waits, a socket reader on the only carrier gets its byte. */
    @Test
    void readerServedWhileSectionWaits() throws Exception {
        assumePool();
        assertReaderServedWhileSectionWaits();
    }

    private void assertReaderServedWhileSectionWaits() throws Exception {
        Holder h = holder(false);
        h.awaitEntered();
        Socket[] pair = connect();
        Reader r = new Reader(pair[1].getInputStream());
        r.awaitParked();
        pair[0].getOutputStream().write(7);
        r.assertGot(7, "reader while the only carrier's section waits");
        assertFalse(h.returned, "holder returned early: the test proved nothing");
        assertTrue(MpscTestSupport.poolThreads().size() >= 1, "no pool thread while a section waits");
    }

    /**
     * A Selector.select with nothing ready and a child's stdout read with nothing to read each free
     * the only carrier.
     */
    @Test
    void realWaitingSitesMove() throws Exception {
        assumePool();
        Selector sel = sun.nio.ch.PollSelectorProvider.provider().openSelector();
        closeables.add(sel);
        Pipe pipe = Pipe.open();
        closeables.add(pipe.source());
        closeables.add(pipe.sink());
        pipe.source().configureBlocking(false);
        pipe.source().register(sel, SelectionKey.OP_READ);
        AtomicInteger selected = new AtomicInteger(-1);
        Thread selector = Thread.ofVirtual().start(() -> {
            try {
                selected.set(sel.select());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        awaitBlockedInNative(selector, "PollSelectorImpl");
        MpscTestSupport.assertRunsNow("a new thread while a select waits");
        sel.wakeup();
        assertTrue(selector.join(Duration.ofMillis(DEADLINE_MS)), "select did not return");
        assertEquals(0, selected.get());

        Process child = new ProcessBuilder("sleep", "30").start();
        closeables.add(child::destroyForcibly);
        InputStream out = child.getInputStream();
        AtomicInteger got = new AtomicInteger(-2);
        Thread reader = Thread.ofVirtual().start(() -> {
            try {
                got.set(out.read());
            } catch (IOException e) {
                got.set(-3);
            }
        });
        // in Mode 4 the read parks on the carrier's poller (MpscPipeWaitTest), else it moves
        MpscTestSupport.awaitParkedOrIn(reader, "readBytes");
        MpscTestSupport.assertRunsNow("a new thread while a child's stdout read waits");
        child.destroyForcibly();
        assertTrue(reader.join(Duration.ofMillis(DEADLINE_MS)), "pipe read did not return");
    }

    /** Mode 4: a pinned socket read waits in a system-wide poller and returns the byte written. */
    @Test
    void pinnedSocketRead() throws Exception {
        // Mode 2 read pollers are virtual threads: they cannot run on the pinned reader's carrier
        assumeTrue(MPSC && System.getProperty("jdk.pollerMode", "4").equals("4"), "MPSC, Mode 4");
        Socket[] pair = connect();
        InputStream in = pair[1].getInputStream();
        AtomicInteger got = new AtomicInteger(-2);
        Thread reader = Thread.ofVirtual().start(() -> {
            try {
                VThreadPinner.runPinned(() -> got.set(in.read()));
            } catch (IOException e) {
                got.set(-3);
            }
        });
        MpscTestSupport.awaitParked(reader);
        pair[0].getOutputStream().write(9);
        assertTrue(reader.join(Duration.ofMillis(DEADLINE_MS)), "the pinned read did not return");
        assertEquals(9, got.get());
        MpscTestSupport.assertRunsNow("a new thread after the pinned read");
    }

    // ---- operations that do not wait move nothing ----

    /**
     * Operations that do not wait (writes with room, reads with data, ready selects) start no pool
     * thread; a waiting section does. A System.out-like stream classifies its fd in a section at
     * its first write, so each stream writes once before.
     */
    @Test
    void quickOperationsMoveNothing() throws Exception {
        assumeTrue(MPSC, "MPSC only");
        byte[] line = "0123456789abcdef0123456789abcdef0123456789abcdef012345678901234\n".getBytes();
        Path fifo = MpscTestSupport.mkfifo();
        try (FileOutputStream devNull = new FileOutputStream("/dev/null");
             RandomAccessFile end = new RandomAccessFile(fifo.toFile(), "rw");
             FileOutputStream w = new FileOutputStream(fifo.toFile())) {
            OutputStream nullOut = systemOut(devNull.getFD());
            OutputStream fifoOut = systemOut(w.getFD());
            VThreadRunner.run(() -> {
                nullOut.write(line);
                fifoOut.write(line);
            });
            MpscTestSupport.awaitNoPoolThreads();
            VThreadRunner.run(() -> quickOperations(line, nullOut, fifoOut));
        }
        assertEquals(List.of(), MpscTestSupport.poolThreads(), "quick operations started pool threads");

        if (CAP > 0) {
            Holder h = holder(false);
            h.awaitEntered();
            assertFalse(MpscTestSupport.poolThreads().isEmpty(), "control: a waiting section started no pool thread");
        }
    }

    private void quickOperations(byte[] line, OutputStream nullOut, OutputStream fifoOut)
            throws Exception {
        // 500 x 64 B = 32 KiB: less than a pipe's 64 KiB, so no write can find it full
        for (int i = 0; i < 500; i++) {
            nullOut.write(line);
        }
        for (int i = 0; i < 499; i++) {
            fifoOut.write(line);
        }
        Process cat = new ProcessBuilder("cat").redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        try (OutputStream in = cat.getOutputStream()) {
            for (int i = 0; i < 500; i++) {
                in.write(line);
                in.flush();
            }
        }
        assertEquals(0, cat.waitFor());

        Process head = new ProcessBuilder("sh", "-c", "head -c 4000 /dev/zero; sleep 30").start();
        try {
            InputStream out = head.getInputStream();
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
            while (out.available() < 4000) {
                assertTrue(System.nanoTime() < end, "child wrote " + out.available());
                Thread.sleep(5);
            }
            byte[] b = new byte[100];
            int total = 0;
            while (total < 4000) {
                int n = out.read(b, 0, Math.min(b.length, 4000 - total));
                assertTrue(n > 0);
                total += n;
            }
        } finally {
            head.destroyForcibly().waitFor();
        }

        try (Selector sel = sun.nio.ch.PollSelectorProvider.provider().openSelector()) {
            Pipe pipe = Pipe.open();
            try (var src = pipe.source(); var sink = pipe.sink()) {
                src.configureBlocking(false);
                src.register(sel, SelectionKey.OP_READ);
                sink.write(ByteBuffer.wrap(new byte[] { 1 }));
                for (int i = 0; i < 500; i++) {
                    assertEquals(1, sel.select());
                    sel.selectedKeys().clear();
                }
            }
        }
    }

    /**
     * Uncontended FileChannel.lock starts no pool thread; a lock another process holds waits on
     * one.
     */
    @Test
    void fileLockMovesOnlyWhenHeld() throws Exception {
        assumeTrue(MPSC && KEEP_ALIVE_MS <= 2000, "MPSC with a short keep-alive");
        MpscTestSupport.awaitNoPoolThreads();
        Path f = Files.createTempFile(Path.of("."), "lock", ".bin");
        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            AtomicReference<Throwable> failed = new AtomicReference<>();
            Thread t = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < 200; i++) {
                        fc.lock(0, 1, (i & 1) == 0).release();
                    }
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
            assertNull(failed.get(), () -> "lock failed: " + failed.get());
            assertEquals(List.of(), MpscTestSupport.poolThreads(), "locks nobody held started pool threads");

            assumeTrue(CAP > 0, "a held lock needs a pool thread");
            Process holder = ProcessTools.createTestJavaProcessBuilder(
                    LockHolder.class.getName(), f.toString()).start();
            closeables.add(holder::destroyForcibly);
            var lines = new java.io.BufferedReader(new java.io.InputStreamReader(holder.getInputStream()));
            assertEquals("locked", lines.readLine());
            AtomicBoolean taken = new AtomicBoolean();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try (var lock = fc.lock(0, 1, false)) {
                    taken.set(lock.isValid());
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            awaitBlockedInNative(waiter, "lock0");
            MpscTestSupport.assertRunsNow("a new thread while a held lock waits");
            assertFalse(MpscTestSupport.poolThreads().isEmpty(), "a held lock waited with no pool thread");
            holder.getOutputStream().close();   // the holder exits, and its lock goes
            assertTrue(waiter.join(Duration.ofMillis(DEADLINE_MS)), "the lock was not taken");
            assertNull(failed.get(), () -> "lock failed: " + failed.get());
            assertTrue(taken.get());
            assertEquals(0, holder.waitFor());
        } finally {
            Files.delete(f);
        }
    }

    /** Holds a lock on the first byte of args[0] until its stdin ends. */
    public static final class LockHolder {
        public static void main(String[] args) throws Exception {
            try (FileChannel fc = FileChannel.open(Path.of(args[0]), StandardOpenOption.WRITE);
                 var lock = fc.lock(0, 1, false)) {
                System.out.println("locked");
                System.out.flush();
                while (System.in.read() >= 0) { }
            }
        }
    }

    /** Zero-length writes to System$Out start no pool thread; a bad range still throws. */
    @Test
    void zeroLengthWritesMoveNothing() throws Exception {
        assumeTrue(MPSC && KEEP_ALIVE_MS <= 2000, "MPSC with a short keep-alive");
        MpscTestSupport.awaitNoPoolThreads();
        byte[] b = new byte[8];
        Path fifo = MpscTestSupport.mkfifo();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        try (FileOutputStream devNull = new FileOutputStream("/dev/null");
             RandomAccessFile fifoEnd = new RandomAccessFile(fifo.toFile(), "rw");
             FileOutputStream fifoOut = new FileOutputStream(fifo.toFile())) {
            List<OutputStream> outs = List.of(systemOut(devNull.getFD()), systemOut(fifoOut.getFD()));
            Thread t = Thread.ofVirtual().start(() -> {
                try {
                    for (OutputStream out : outs) {
                        for (int i = 0; i < 100; i++) {
                            out.write(b, i & 7, 0);
                            out.write(new byte[0]);
                        }
                        assertThrows(IndexOutOfBoundsException.class, () -> out.write(b, 9, 0));
                    }
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
            assertNull(failed.get(), () -> "write failed: " + failed.get());
        }
        assertEquals(List.of(), MpscTestSupport.poolThreads(), "zero-length writes started pool threads");
    }

    // ---- disk I/O on a local file stays on the carrier ----

    /**
     * DSYNC and rwd writes, force, FileDescriptor.sync and MappedByteBuffer.force on a local file
     * start no pool thread.
     */
    @Test
    void diskIoMovesNothing() throws Exception {
        assumeTrue(MPSC, "MPSC only");
        Path f = Files.createTempFile(localDir(), "disk", ".bin");
        try (FileChannel fc = FileChannel.open(f, StandardOpenOption.READ, StandardOpenOption.WRITE,
                     StandardOpenOption.DSYNC);
             RandomAccessFile raf = new RandomAccessFile(f.toFile(), "rwd")) {
            // each fd's kind is read in a section at its first synchronous operation
            VThreadRunner.run(() -> {
                fc.force(true);
                raf.getFD().sync();
            });
            MpscTestSupport.awaitNoPoolThreads();
            AtomicReference<Throwable> failed = new AtomicReference<>();
            Thread t = Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        assertEquals(8, fc.write(ByteBuffer.allocate(8).putLong(0, i), 0));
                        fc.force(true);
                        raf.seek(8);
                        raf.write(i);
                        raf.getFD().sync();
                    }
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
            assertNull(failed.get(), () -> "disk I/O failed: " + failed.get());
            assertEquals(List.of(), MpscTestSupport.poolThreads(), "disk I/O started pool threads");

            Thread m = Thread.ofVirtual().start(() -> {
                try (FileChannel mc = FileChannel.open(f, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    var map = mc.map(FileChannel.MapMode.READ_WRITE, 0, 16);
                    map.put(9, (byte) 42);
                    map.force();
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            assertTrue(m.join(Duration.ofMillis(DEADLINE_MS)), "MappedByteBuffer.force did not complete");
            assertNull(failed.get(), () -> "MappedByteBuffer.force failed: " + failed.get());
            byte[] b = Files.readAllBytes(f);
            assertEquals(99, ByteBuffer.wrap(b).getLong(0));
            assertEquals(42, b[9]);
        } finally {
            Files.delete(f);
        }

        if (CAP > 0) {
            Holder h = holder(false);
            h.awaitEntered();
            assertFalse(MpscTestSupport.poolThreads().isEmpty(), "control: a waiting section started no pool thread");
        }
    }

    /**
     * An "rws" write to a FIFO larger than the pipe waits for a reader without holding the carrier.
     */
    @Test
    void syncWriteToAFifoDoesNotHoldTheCarrier() throws Exception {
        assumePool();
        Path fifo = MpscTestSupport.mkfifo();
        // a pipe holds 16 pages: 64 KiB with 4 KiB pages, 1 MiB with 64 KiB pages
        byte[] data = new byte[2 * 1024 * 1024];
        // O_RDWR on both ends: neither open waits for the other
        try (RandomAccessFile out = new RandomAccessFile(fifo.toFile(), "rws");
             RandomAccessFile in = new RandomAccessFile(fifo.toFile(), "rw")) {
            CountDownLatch entered = new CountDownLatch(1);
            AtomicReference<Throwable> failed = new AtomicReference<>();
            Thread writer = Thread.ofVirtual().start(() -> {
                entered.countDown();
                try {
                    out.write(data);
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            try {
                assertTrue(entered.await(DEADLINE_MS, TimeUnit.MILLISECONDS), "writer did not start");
                // the write cannot complete before the reads below: only a writer that
                // left the only carrier lets a new thread run
                MpscTestSupport.assertRunsNow("a new thread while an rws write to a full FIFO waits");
                assertTrue(writer.isAlive(), "the write returned with no reader: the test proved nothing");
            } finally {
                if (failed.get() == null) {
                    in.readFully(new byte[data.length]);
                }
            }
            assertTrue(writer.join(Duration.ofMillis(DEADLINE_MS)), "the write did not complete");
            assertNull(failed.get(), () -> "the write failed: " + failed.get());
        } finally {
            Files.delete(fifo);
        }
    }

    /** An interrupt ends a moved select at once, with the interrupt status set, as on FJP. */
    @Test
    void interruptEndsAMovedSelect() throws Exception {
        try (Selector sel = sun.nio.ch.PollSelectorProvider.provider().openSelector()) {
            AtomicBoolean interrupted = new AtomicBoolean();
            AtomicInteger n = new AtomicInteger(-1);
            Thread t = Thread.ofVirtual().start(() -> {
                try {
                    n.set(sel.select());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                interrupted.set(Thread.currentThread().isInterrupted());
            });
            awaitBlockedInNative(t, "PollSelectorImpl");
            t.interrupt();
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)), "select did not end");
            assertTrue(interrupted.get(), "interrupt status lost");
            assertEquals(0, n.get());
            assertTrue(sel.isOpen());
        }
    }

    /**
     * An exception thrown inside a moved section reaches the caller, and the thread ends on a
     * carrier.
     */
    @Test
    void exceptionInsideAMovedSection() throws Exception {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                boolean a = Blocker.begin();
                try {
                    throw new IOException("inside the section");
                } finally {
                    Blocker.end(a);
                }
            } catch (Throwable e) {
                thrown.set(e);
            }
        });
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
        Throwable e = thrown.get();
        assertInstanceOf(IOException.class, e);
        boolean callerFrame = false;
        for (StackTraceElement f : e.getStackTrace()) {
            callerFrame |= f.getClassName().startsWith(MpscOffloadTest.class.getName());
        }
        assertTrue(callerFrame, "the trace lost the caller's frames");
        assertTrue(MpscTestSupport.awaitPoolThreadsParked(), "a pool thread still runs: " + MpscTestSupport.poolThreads());
    }

    // ---- leaving the pool thread other than at the end ----

    /**
     * A thread that parks inside its section, or ends it pinned, ends up back on a carrier with no
     * pool thread left.
     */
    @Test
    void leavingThePoolThreadEarlyOrLate() throws Exception {
        assumePool();
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<String> afterPark = new AtomicReference<>();
        Thread parker = Thread.ofVirtual().start(() -> {
            boolean a = Blocker.begin();
            try {
                release.await();
                afterPark.set(MpscTestSupport.carrierName());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                Blocker.end(a);
            }
        });
        MpscTestSupport.awaitParked(parker);
        release.countDown();
        assertTrue(parker.join(Duration.ofMillis(DEADLINE_MS)), "parker did not finish");
        assertTrue(afterPark.get().startsWith(MpscTestSupport.CARRIER),
                "a park inside a section did not leave the pool thread: " + afterPark.get());

        AtomicReference<String> afterPinnedEnd = new AtomicReference<>();
        AtomicReference<String> afterYield = new AtomicReference<>();
        Thread pinnedEnd = Thread.ofVirtual().start(() -> {
            boolean a = Blocker.begin();
            VThreadPinner.runPinned(() -> Blocker.end(a));
            afterPinnedEnd.set(MpscTestSupport.carrierName());
            Thread.yield();   // unmounts: back to its scheduler
            afterYield.set(MpscTestSupport.carrierName());
        });
        assertTrue(pinnedEnd.join(Duration.ofMillis(DEADLINE_MS)), "pinned-end thread did not finish");
        assertTrue(afterPinnedEnd.get().startsWith(MpscTestSupport.POOL_THREAD),
                "a pinned end left the pool thread: " + afterPinnedEnd.get());
        assertTrue(afterYield.get().startsWith(MpscTestSupport.CARRIER),
                "not back on a carrier after unmounting: " + afterYield.get());
        assertTrue(MpscTestSupport.awaitPoolThreadsParked(), "a pool thread still runs: " + MpscTestSupport.poolThreads());
        assertReaderServedWhileSectionWaits();
    }

    // ---- the pool's bound, keep-alive and interrupt ----

    /**
     * cap waiting sections hold cap pool threads and one more runs on its carrier; all complete and
     * the next one moves.
     */
    @Test
    void boundedByCap() throws Exception {
        assumeTrue(MPSC && CAP > 0 && CAP <= 16, "a small pool bound");
        List<Holder> hs = new ArrayList<>();
        for (int i = 0; i < CAP; i++) {
            Holder h = holder(false);
            h.awaitEntered();
            hs.add(h);
        }
        assertEquals(CAP, MpscTestSupport.poolThreads().size());
        Holder extra = holder(false);   // no pool thread for it: runs on the carrier
        extra.awaitEntered();
        assertEquals(CAP, MpscTestSupport.poolThreads().size(), "the pool went past its bound");
        extra.releaseAndJoin();
        for (Holder h : hs) {
            h.releaseAndJoin();
        }
        closeables.removeAll(hs);
        closeables.remove(extra);
        assertReaderServedWhileSectionWaits();
        assertTrue(MpscTestSupport.poolThreads().size() <= CAP);
    }

    /**
     * At the cap a section runs on its carrier at once: a thread queued behind it runs after the
     * section.
     */
    @Test
    void atTheCapASectionRunsAtOnce() throws Exception {
        assumeTrue(MPSC && CAP == 1, "a pool bound of 1");
        Holder h = holder(false);
        h.awaitEntered();
        assertEquals(1, MpscTestSupport.poolThreads().size());
        AtomicBoolean sectionDone = new AtomicBoolean();
        AtomicReference<Boolean> seenByQueued = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            // queued on this, the only, carrier: it runs when this thread unmounts
            Thread.ofVirtual().start(() -> seenByQueued.set(sectionDone.get()));
            boolean a = Blocker.begin();
            try {
                sectionDone.set(true);
            } finally {
                Blocker.end(a);
            }
        });
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        while (seenByQueued.get() == null) {
            assertTrue(System.nanoTime() < end, "the queued thread did not run");
            Thread.sleep(1);
        }
        assertTrue(seenByQueued.get(), "the section yielded at the cap: a queued thread ran before it");
        h.releaseAndJoin();
        closeables.remove(h);
    }

    /**
     * A pinned thread's section runs on its carrier and JFR records a jdk.VirtualThreadPinned
     * event.
     */
    @Test
    void pinnedSectionIsRecorded() throws Exception {
        assumePool();
        assumeTrue(FlightRecorder.isAvailable(), "JFR");
        Path file = Files.createTempFile(Path.of("."), "pinned", ".jfr");
        Thread t;
        try (Recording r = new Recording()) {
            r.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO);
            r.start();
            t = Thread.ofVirtual().start(() -> VThreadPinner.runPinned(() -> {
                boolean a = Blocker.begin();
                Blocker.end(a);
            }));
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
            r.stop();
            r.dump(file);
        }
        List<RecordedEvent> events = RecordingFile.readAllEvents(file).stream()
                .filter(e -> e.getThread() != null && e.getThread().getJavaThreadId() == t.threadId())
                .toList();
        Files.delete(file);
        assertFalse(events.isEmpty(), "no event of the pinned section");
    }

    /**
     * With jdk.FileWrite enabled, each write to a subprocess pipe or System$Out records one event
     * with the whole write's bytes.
     */
    @Test
    void pipeWritesRecordOneFileWriteEach() throws Exception {
        assumeTrue(FlightRecorder.isAvailable(), "JFR");
        // a pipe holds 16 pages: 64 KiB with 4 KiB pages, 1 MiB with 64 KiB pages
        int[] sizes = { 1, 100, 8191, 8192, 65536, 2 * 1024 * 1024, 7 };
        Path file = Files.createTempFile(Path.of("."), "filewrite", ".jfr");
        Process cat = new ProcessBuilder("cat").redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        List<Long> expected = new ArrayList<>();
        Thread t;
        try (Recording r = new Recording()) {
            r.enable("jdk.FileWrite").withThreshold(Duration.ZERO);
            r.start();
            t = Thread.ofVirtual().start(() -> {
                try (OutputStream in = cat.getOutputStream();
                     FileOutputStream devNull = new FileOutputStream("/dev/null")) {
                    OutputStream out = systemOut(devNull.getFD());
                    for (OutputStream o : List.of(in, out)) {
                        for (int n : sizes) {
                            o.write(new byte[n]);
                            o.flush();
                            expected.add((long) n);
                        }
                    }
                    out.write(1);
                    expected.add(1L);
                } catch (Throwable e) {
                    failed.set(e);
                }
            });
            assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)), "the writes did not complete");
            r.stop();
            r.dump(file);
        }
        assertNull(failed.get(), () -> "a write failed: " + failed.get());
        assertEquals(0, cat.waitFor());
        List<Long> written = RecordingFile.readAllEvents(file).stream()
                .filter(e -> e.getEventType().getName().equals("jdk.FileWrite"))
                .filter(e -> e.getThread() != null && e.getThread().getJavaThreadId() == t.threadId())
                .sorted(Comparator.comparing(RecordedEvent::getStartTime))
                .map(e -> e.getLong("bytesWritten"))
                .toList();
        Files.delete(file);
        assertEquals(expected, written, "bytesWritten of the writer's jdk.FileWrite events, in order");
    }

    /** No pool allowed: sections run on the carrier, and complete. */
    @Test
    void noPoolRunsSectionsOnTheCarrier() throws Exception {
        assumeTrue(MPSC && CAP == 0, "maxPoolSize = parallelism");
        Holder h = holder(false);
        h.awaitEntered();
        assertEquals(List.of(), MpscTestSupport.poolThreads());
        h.releaseAndJoin();
        closeables.remove(h);
    }

    /** A burst's pool threads exit after the keep-alive; then a section still moves. */
    @Test
    void idlePoolThreadsExitAfterTheKeepAlive() throws Exception {
        assumeTrue(MPSC && CAP >= 8 && KEEP_ALIVE_MS <= 2000, "a short keep-alive");
        List<Holder> hs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Holder h = holder(false);
            h.awaitEntered();
            hs.add(h);
        }
        assertTrue(MpscTestSupport.poolThreads().size() >= 8);
        for (Holder h : hs) {
            h.releaseAndJoin();
        }
        closeables.removeAll(hs);
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(KEEP_ALIVE_MS * 3 + DEADLINE_MS);
        while (!MpscTestSupport.poolThreads().isEmpty()) {
            assertTrue(System.nanoTime() < end, "pool threads did not exit: " + MpscTestSupport.poolThreads());
            Thread.sleep(20);
        }
        assertReaderServedWhileSectionWaits();
    }

    /** An interrupted idle pool thread clears its interrupt status and parks again. */
    @Test
    void interruptedIdlePoolThreadsParkAgain() throws Exception {
        assumePool();
        MpscTestSupport.awaitNoPoolThreads();
        Holder h = holder(false);
        h.awaitEntered();
        h.releaseAndJoin();
        closeables.remove(h);
        assertTrue(MpscTestSupport.awaitPoolThreadsParked());
        List<Thread> pool = MpscTestSupport.poolThreads();
        assertFalse(pool.isEmpty());
        for (Thread t : pool) {
            t.interrupt();
        }
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        for (Thread t : pool) {
            // one that cleared its status may also have parked and then exited at its keep-alive
            while (t.isInterrupted() || (t.getState() != Thread.State.TIMED_WAITING && t.isAlive())) {
                assertTrue(System.nanoTime() - end < 0 && (t.isAlive() || !t.isInterrupted()),
                        t.getName() + " did not park again: " + t.getState() + ", interrupted=" + t.isInterrupted());
                Thread.sleep(1);
            }
        }
    }

    /**
     * 1, 2 and 4 threads making short sections in sequence leave no more pool threads alive than
     * there are threads.
     */
    @Test
    void sequentialSectionsNeedOnePoolThreadEach() throws Exception {
        assumeTrue(MPSC && CAP >= 4 && KEEP_ALIVE_MS <= 2000, "room for 4 and a short keep-alive");
        for (int k : new int[] { 1, 2, 4 }) {
            MpscTestSupport.awaitNoPoolThreads();
            Set<String> ranOn = ConcurrentHashMap.newKeySet();
            List<Thread> ts = new ArrayList<>();
            for (int i = 0; i < k; i++) {
                ts.add(Thread.ofVirtual().start(() -> {
                    for (int j = 0; j < 10_000 / k; j++) {
                        boolean a = Blocker.begin();
                        ranOn.add(MpscTestSupport.carrierName());
                        Blocker.end(a);
                    }
                }));
            }
            for (Thread t : ts) {
                assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)), "sections did not finish");
            }
            assertTrue(ranOn.stream().anyMatch(n -> n.startsWith(MpscTestSupport.POOL_THREAD)),
                    "no section moved: the test proved nothing " + ranOn);
            List<Thread> alive = MpscTestSupport.poolThreads();
            assertTrue(alive.size() <= k, k + " threads left " + alive);
        }
    }

    /**
     * Two carriers, a pool bound of 8: 8 sections begun together all wait on pool threads
     * at once, which the pool starts one from the other, and a new thread runs meanwhile.
     * A 9th section, at the cap, waits on its carrier, and completes once released.
     */
    @Test
    void aBurstIsPlacedAndTheCapHolds() throws Exception {
        assumeTrue(MPSC && MpscTestSupport.PARALLELISM == 2 && CAP == 8, "p=2 and a pool bound of 8");
        MpscTestSupport.awaitNoPoolThreads();
        List<Holder> hs = new ArrayList<>();
        for (int i = 0; i < CAP; i++) {
            hs.add(holder(false));
        }
        for (Holder h : hs) {
            h.awaitEntered();
            assertTrue(h.carrier.startsWith(MpscTestSupport.POOL_THREAD),
                    "a section of the burst stayed on " + h.carrier);
        }
        assertEquals(CAP, MpscTestSupport.poolThreads().size());
        MpscTestSupport.assertRunsNow("a new thread while 8 sections wait");

        Holder ninth = holder(false);
        ninth.awaitEntered();
        assertTrue(ninth.carrier.startsWith(MpscTestSupport.CARRIER),
                "a section past the cap moved to " + ninth.carrier);
        assertEquals(CAP, MpscTestSupport.poolThreads().size(), "the pool went past its bound");
        ninth.releaseAndJoin();
        for (Holder h : hs) {
            h.releaseAndJoin();
        }
        closeables.removeAll(hs);
        closeables.remove(ninth);
    }

    // ---- the offload hint: only a section's begin moves a thread ----

    /** Plain Thread.yield() calls start no pool thread. */
    @Test
    void plainYieldsMoveNothing() throws Exception {
        assumeTrue(MPSC && CAP > 0 && KEEP_ALIVE_MS <= 2000, "a pool and a short keep-alive");
        MpscTestSupport.awaitNoPoolThreads();
        AtomicReference<String> where = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < 1000; i++) {
                Thread.yield();
                if (!MpscTestSupport.carrierName().startsWith(MpscTestSupport.CARRIER)) {
                    where.set(MpscTestSupport.carrierName());
                }
            }
        });
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
        assertNull(where.get(), () -> "a plain yield moved the thread to " + where.get());
        assertEquals(List.of(), MpscTestSupport.poolThreads(), "plain yields started pool threads");
    }

    /**
     * After a pinned thread's section, its plain Thread.yield() keeps it on a carrier; its next
     * section moves as usual.
     */
    @Test
    void aYieldAfterAPinnedSectionStaysOnACarrier() throws Exception {
        assumePool();
        AtomicReference<String> inside = new AtomicReference<>();
        AtomicReference<String> afterYield = new AtomicReference<>();
        AtomicReference<String> nextSection = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            VThreadPinner.runPinned(() -> {
                boolean a = Blocker.begin();
                try {
                    inside.set(MpscTestSupport.carrierName());
                } finally {
                    Blocker.end(a);
                }
            });
            Thread.yield();
            afterYield.set(MpscTestSupport.carrierName());
            boolean a = Blocker.begin();
            try {
                nextSection.set(MpscTestSupport.carrierName());
            } finally {
                Blocker.end(a);
            }
        });
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS)));
        assertTrue(inside.get().startsWith(MpscTestSupport.CARRIER), "the pinned section moved: " + inside.get());
        assertTrue(afterYield.get().startsWith(MpscTestSupport.CARRIER),
                "the yield after a pinned section moved the thread to " + afterYield.get());
        assertTrue(nextSection.get().startsWith(MpscTestSupport.POOL_THREAD), "the next section did not move: " + nextSection.get());
    }

    // ---- helpers ----

    /** A virtual thread in a Blocker section waiting in a FIFO read until released. */
    private Holder holder(boolean pinned) throws Exception {
        Holder h = new Holder(pinned);
        closeables.add(h);
        return h;
    }

    private static final class Holder implements AutoCloseable {
        final Path fifo;
        final RandomAccessFile writer;
        final CountDownLatch entered = new CountDownLatch(1);
        final Thread thread;
        volatile String carrier;
        volatile boolean returned;
        private boolean released;

        Holder(boolean pinned) throws Exception {
            fifo = MpscTestSupport.mkfifo();
            // O_RDWR on both ends: neither open waits for the other
            writer = new RandomAccessFile(fifo.toFile(), "rw");
            RandomAccessFile reader = new RandomAccessFile(fifo.toFile(), "rw");
            Runnable body = () -> {
                boolean a = Blocker.begin();
                try {
                    carrier = MpscTestSupport.carrierName();
                    entered.countDown();
                    reader.read();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    Blocker.end(a);
                    returned = true;
                }
            };
            thread = Thread.ofVirtual().start(() -> {
                try (reader) {
                    if (pinned) {
                        VThreadPinner.runPinned(body::run);
                    } else {
                        body.run();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        void awaitEntered() throws InterruptedException {
            assertTrue(entered.await(DEADLINE_MS, TimeUnit.MILLISECONDS), "holder did not enter");
        }

        void releaseAndJoin() throws Exception {
            if (!released) {
                released = true;
                writer.write(1);
                assertTrue(thread.join(Duration.ofMillis(DEADLINE_MS)), "holder did not finish");
                writer.close();
                Files.deleteIfExists(fifo);
            }
        }

        @Override
        public void close() throws Exception {
            releaseAndJoin();
        }
    }

    private static final class Reader {
        final Thread thread;
        final AtomicInteger got = new AtomicInteger(-2);

        Reader(InputStream in) {
            thread = Thread.ofVirtual().start(() -> {
                try {
                    got.set(in.read());
                } catch (IOException e) {
                    got.set(-3);
                }
            });
        }

        void awaitParked() throws InterruptedException {
            MpscTestSupport.awaitParked(thread);
        }

        void assertGot(int b, String what) throws InterruptedException {
            assertTrue(thread.join(Duration.ofMillis(DEADLINE_MS)), what + ": not served");
            assertEquals(b, got.get(), what);
        }
    }

    private Socket[] connect() throws IOException {
        Socket a = new Socket(listener.getInetAddress(), listener.getLocalPort());
        Socket b = listener.accept();
        closeables.add(a);
        closeables.add(b);
        return new Socket[] { a, b };
    }

    /**
     * A directory on a local filesystem: /dev/shm if tmpfs, else the test's directory if of a
     * common local type.
     */
    private static Path localDir() throws IOException {
        Path shm = Path.of("/dev/shm");
        if (Files.isDirectory(shm) && Files.getFileStore(shm).type().equals("tmpfs")) {
            return shm;
        }
        Path dot = Path.of(".");
        String type = Files.getFileStore(dot).type();
        assumeTrue(List.of("ext2", "ext3", "ext4", "xfs", "btrfs", "tmpfs").contains(type),
                "no local filesystem known: . is " + type);
        return dot;
    }

    /** A System$Out on fd, the class behind System.out (input only, built by reflection). */
    private static OutputStream systemOut(FileDescriptor fd) throws Exception {
        Constructor<?> c = Class.forName("java.lang.System$Out").getDeclaredConstructor(FileDescriptor.class);
        c.setAccessible(true);
        return (OutputStream) c.newInstance(fd);
    }

    /** Waits until t's stack shows it inside the named call (a frame whose text contains it). */
    private static void awaitBlockedInNative(Thread t, String frame) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        for (;;) {
            for (StackTraceElement f : t.getStackTrace()) {
                if (f.toString().contains(frame)) {
                    return;
                }
            }
            assertTrue(System.nanoTime() < end, t + " never reached " + frame);
            Thread.sleep(2);
        }
    }
}
