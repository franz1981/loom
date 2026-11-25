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
 * @summary MPSC scheduler: an error that escapes a task goes to the carrier's
 *          uncaught-exception handler, and the carrier goes on running threads
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscCarrierTaskErrorTest
 */

/*
 * @test id=nosteal
 * @requires vm.continuations
 * @modules java.base/java.lang:+open
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.virtualThreadScheduler.parallelism=1 MpscCarrierTaskErrorTest
 */

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jdk.test.lib.thread.VThreadScheduler;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MpscCarrierTaskErrorTest {
    private static final long DEADLINE_MS = MpscTestSupport.DEADLINE_MS;

    private record Uncaught(Thread thread, Throwable error) { }

    /**
     * An error thrown out of a task, by the virtual thread's own uncaught-exception
     * handler, reaches the carrier's handler, and the carrier then runs a new thread.
     */
    @Test
    void testErrorOutOfATaskGoesToTheCarrier() throws Exception {
        AtomicReference<Thread> carrierRef = new AtomicReference<>();
        Thread.ofVirtual().start(() -> carrierRef.set(VThreadScheduler.currentCarrierThread())).join();
        Thread carrier = carrierRef.get();
        assertTrue(carrier.getName().startsWith("mpsc-carrier-"), "not an MPSC carrier: " + carrier);

        BlockingQueue<Uncaught> seen = new ArrayBlockingQueue<>(10);
        Thread.UncaughtExceptionHandler previous = carrier.getUncaughtExceptionHandler();
        carrier.setUncaughtExceptionHandler((t, e) -> seen.add(new Uncaught(t, e)));
        try {
            Error error = new Error("thrown by the virtual thread's uncaught-exception handler");
            Thread vthread = Thread.ofVirtual()
                    .uncaughtExceptionHandler((t, e) -> { throw error; })
                    .start(() -> { throw new RuntimeException("thrown by the task"); });

            Uncaught u = seen.poll(DEADLINE_MS, TimeUnit.MILLISECONDS);
            assertNotNull(u, "the carrier's handler saw nothing");
            assertSame(carrier, u.thread());
            assertSame(error, u.error());
            assertTrue(vthread.join(Duration.ofMillis(DEADLINE_MS)), "the virtual thread did not terminate");

            for (int i = 0; i < 3; i++) {
                Thread next = Thread.ofVirtual().start(() -> { });
                assertTrue(next.join(Duration.ofMillis(DEADLINE_MS)), "a new thread did not run after the error");
            }
            assertTrue(carrier.isAlive(), "the carrier ended");
            assertNull(seen.poll(), "more errors reached the carrier");
        } finally {
            carrier.setUncaughtExceptionHandler(previous);
        }
    }
}
