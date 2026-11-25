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
 * @test id=mode1-steal
 * @summary An interrupted, idle MPSC carrier clears its interrupt status and parks again
 *          (without a local poller), and the scheduler still runs threads
 * @requires vm.continuations
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscCarrierInterruptTest
 */

/*
 * @test id=mode1-nosteal
 * @requires vm.continuations
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=1
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscCarrierInterruptTest
 */

/*
 * @test id=mode4-steal
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscCarrierInterruptTest
 */

/*
 * @test id=mode4-nosteal
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscCarrierInterruptTest
 */

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MpscCarrierInterruptTest {
    private static final int POLLER_MODE = Integer.getInteger("jdk.pollerMode");

    /**
     * A carrier without a local poller parks when idle: interrupted, it clears the status
     * and parks again. A carrier in Mode 4 waits in its poller, which an interrupt does
     * not end, so for it only the last part applies.
     */
    @Test
    void interruptedIdleCarrierParksAgain() throws Exception {
        // start the carriers
        Thread.ofVirtual().start(() -> { }).join();
        List<Thread> carriers = Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getName().startsWith("mpsc-carrier-"))
                .toList();
        assertFalse(carriers.isEmpty(), "no MPSC carrier found");
        for (Thread c : carriers) {
            c.interrupt();
        }
        if (POLLER_MODE != 4) {
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MpscTestSupport.DEADLINE_MS);
            for (Thread c : carriers) {
                while (c.isInterrupted() || c.getState() != Thread.State.WAITING) {
                    assertTrue(System.nanoTime() - end < 0, c.getName() + " did not park again: "
                            + c.getState() + ", interrupted=" + c.isInterrupted());
                    Thread.sleep(1);
                }
            }
        }

        // the scheduler still runs threads, and a virtual thread's interrupt still works
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                new CountDownLatch(1).await();
                fail("not interrupted");
            } catch (InterruptedException e) { }
        });
        MpscTestSupport.awaitParked(t);
        t.interrupt();
        assertTrue(t.join(Duration.ofMillis(MpscTestSupport.DEADLINE_MS)), "the interrupt did not end the wait");
    }
}
