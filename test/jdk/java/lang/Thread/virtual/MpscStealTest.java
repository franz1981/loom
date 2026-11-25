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
 * @summary MPSC scheduler: an idle carrier runs a thread queued on a busy carrier
 * @requires vm.continuations
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscStealTest
 */

/*
 * @test id=mode2
 * @requires vm.continuations & os.family == "linux"
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscStealTest
 */

/*
 * @test id=nosteal
 * @requires vm.continuations
 * @library /test/lib
 * @run junit/othervm -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.virtualThreadScheduler.parallelism=2 MpscStealTest
 */

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscStealTest {

    /**
     * The started thread is queued on the starter's carrier, and the starter spins without
     * unmounting until it has run, so only another carrier can run it.
     */
    @Test
    void testIdleCarrierTakesAQueuedThread() throws Exception {
        assumeTrue(MpscTestSupport.STEAL, "stealing is off");
        for (int round = 0; round < 10; round++) {
            AtomicBoolean ran = new AtomicBoolean();
            AtomicBoolean seenWhileSpinning = new AtomicBoolean();
            Thread spinner = Thread.ofVirtual().start(() -> {
                Thread.ofVirtual().start(() -> ran.set(true));
                long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MpscTestSupport.DEADLINE_MS);
                while (!ran.get() && System.nanoTime() - end < 0) {
                    Thread.onSpinWait();
                }
                seenWhileSpinning.set(ran.get());
            });
            spinner.join();
            assertTrue(seenWhileSpinning.get(), "round " + round
                    + ": the queued thread did not run while its carrier was busy");
        }
    }
}
