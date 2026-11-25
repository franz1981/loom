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
import java.util.List;
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
