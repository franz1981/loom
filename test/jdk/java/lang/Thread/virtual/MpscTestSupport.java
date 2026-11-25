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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import jdk.test.lib.Utils;

/**
 * Helpers for the MPSC scheduler tests in this directory. Users need
 * {@code @library /test/lib}. Failures are thrown as AssertionError, so that main tests
 * and child JVMs can use the helpers too.
 */
final class MpscTestSupport {
    private MpscTestSupport() { }

    /** Whether the MPSC scheduler steals, as it reads jdk.virtualThreadScheduler.mpscSteal. */
    static final boolean STEAL =
            Boolean.parseBoolean(
                    System.getProperty("jdk.virtualThreadScheduler.mpscSteal", "true"));

    /** A hang guard: how long a wait for something that must happen may last. */
    static final long DEADLINE_MS = Utils.adjustTimeout(10_000);

    /** Whether the built-in scheduler is the MPSC one. */
    static final boolean MPSC = Boolean.getBoolean("jdk.virtualThreadScheduler.useMpsc");

    static final int PARALLELISM = Integer.getInteger("jdk.virtualThreadScheduler.parallelism",
            Runtime.getRuntime().availableProcessors());

    /** The offload pool's bound: maxPoolSize - parallelism. */
    static final int POOL_CAP = Integer.getInteger("jdk.virtualThreadScheduler.maxPoolSize",
            Math.max(PARALLELISM, 256)) - PARALLELISM;

    /** How long an idle offload pool thread lives. */
    static final long KEEP_ALIVE_MS =
            Long.getLong("jdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis", 30_000L);

    /** Name prefixes: the scheduler's carriers and its offload pool threads. */
    static final String CARRIER = "mpsc-carrier-";
    static final String POOL_THREAD = "mpsc-helper-";

    /** Fails unless the scheduler's own asserts are enabled (-esa). */
    static void assertSchedulerAssertionsEnabled() throws ClassNotFoundException {
        Class<?> c = Class.forName("java.lang.MpscVirtualThreadScheduler");
        if (!c.desiredAssertionStatus()) {
            throw new AssertionError("run with -esa");
        }
    }

    /** Waits until the thread is parked (WAITING or TIMED_WAITING); fails at the deadline. */
    static void awaitParked(Thread thread) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        for (;;) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            if (state == Thread.State.TERMINATED || System.nanoTime() - end > 0) {
                throw new AssertionError(thread + " did not park: " + state);
            }
            Thread.sleep(1);
        }
    }

    /**
     * Waits until the thread is parked, or until its stack shows a frame whose text
     * contains one of the given names (a call blocked in a system call shows RUNNABLE);
     * fails at the deadline.
     */
    static void awaitParkedOrIn(Thread thread, String... frames) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        for (;;) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            for (StackTraceElement f : thread.getStackTrace()) {
                for (String frame : frames) {
                    if (f.toString().contains(frame)) {
                        return;
                    }
                }
            }
            if (state == Thread.State.TERMINATED || System.nanoTime() - end > 0) {
                throw new AssertionError(thread + " neither parked nor reached " + List.of(frames)
                        + ": " + state + " " + List.of(thread.getStackTrace()));
            }
            Thread.sleep(1);
        }
    }

    /** Fails unless a new virtual thread runs within the deadline. */
    static void assertRunsNow(String what) throws InterruptedException {
        CountDownLatch ran = new CountDownLatch(1);
        Thread.ofVirtual().start(ran::countDown);
        if (!ran.await(DEADLINE_MS, TimeUnit.MILLISECONDS)) {
            throw new AssertionError(what + ": a new thread did not run");
        }
    }

    /** The name of the current virtual thread's carrier, from its toString. */
    static String carrierName() {
        String s = Thread.currentThread().toString();
        return s.substring(s.lastIndexOf('@') + 1);
    }

    /** The offload pool threads alive now. */
    static List<Thread> poolThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith(POOL_THREAD))
                .toList();
    }

    /** Waits until every pool thread has exited after the keep-alive. */
    static void awaitNoPoolThreads() throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(KEEP_ALIVE_MS * 3 + DEADLINE_MS);
        while (!poolThreads().isEmpty()) {
            if (System.nanoTime() - end > 0) {
                throw new AssertionError("pool threads did not exit: " + poolThreads());
            }
            Thread.sleep(20);
        }
    }

    /** Waits until every pool thread is parked; false at the deadline. */
    static boolean awaitPoolThreadsParked() throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MS);
        while (System.nanoTime() - end < 0) {
            if (poolThreads().stream().allMatch(t -> t.getState() == Thread.State.WAITING
                    || t.getState() == Thread.State.TIMED_WAITING)) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }

    /** A new FIFO in a new directory under the current one. */
    static Path mkfifo() throws IOException, InterruptedException {
        Path fifo = Files.createTempDirectory(Path.of("."), "fifo").resolve("fifo");
        int rc = new ProcessBuilder("mkfifo", fifo.toString()).inheritIO().start().waitFor();
        if (rc != 0) {
            throw new IOException("mkfifo " + rc);
        }
        return fifo;
    }

    /**
     * Starts the threads from one virtual thread, in order: a thread started by a
     * virtual thread starts on the starter's carrier, and without stealing stays there.
     */
    static void startTogether(List<Thread> threads) throws InterruptedException {
        Thread.ofVirtual().start(() -> threads.forEach(Thread::start)).join();
    }

    /** A connected loopback pair: a virtual thread reads, a platform thread writes. */
    record SocketPair(Socket readerSide, Socket writerSide) implements AutoCloseable {
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
}
