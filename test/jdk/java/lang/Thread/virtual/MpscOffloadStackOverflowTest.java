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
 * @summary MPSC scheduler: a StackOverflowError at a Blocker section's begin or end,
 *          with the stack nearly full, leaves the thread able to go on: its next
 *          section moves to an offload pool thread and it comes back to a carrier.
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadStackOverflowTest
 */

/*
 * @test id=mpsc-mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadStackOverflowTest
 */

/*
 * @test id=mpsc-steal-off
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.mpscSteal=false
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.mpscOffloadKeepAliveMillis=1000 MpscOffloadStackOverflowTest
 */

/*
 * @test id=mpsc-cap1
 * @summary The pool's bound: maxPoolSize - parallelism = 1
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm/native -esa --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=2 MpscOffloadStackOverflowTest
 */

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jdk.internal.misc.Blocker;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MpscOffloadStackOverflowTest {
    private static final boolean MPSC = MpscTestSupport.MPSC;
    private static final int CAP = MpscTestSupport.POOL_CAP;
    private static final long DEADLINE_MS = MpscTestSupport.DEADLINE_MS;

    /**
     * A StackOverflowError at a section's begin or end, at each depth near the stack's limit,
     * leaves the thread on a carrier with its next section moving; no pool thread is left running.
     */
    @Test
    void stackOverflowAtASectionsBeginOrEnd() throws Exception {
        assumePool();
        AtomicReference<String> failed = new AtomicReference<>();
        AtomicInteger rounds = new AtomicInteger();
        Thread t = Thread.ofVirtual().start(() -> {
            for (boolean atEnd : new boolean[] { false, true }) {
                for (int k = 0; k <= 150 && failed.get() == null; k++) {
                    // the deepest call of the same recursion, measured each round, as
                    // the JIT may change its frames
                    int target = edgeRound(atEnd, Integer.MAX_VALUE) - k;
                    edgeRound(atEnd, target);
                    rounds.incrementAndGet();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    String where = (atEnd ? "end" : "begin") + " at depth " + target + ": ";
                    if (!MpscTestSupport.carrierName().startsWith(MpscTestSupport.CARRIER)) {
                        failed.set(where + "not on a carrier after an unmount: " + MpscTestSupport.carrierName());
                    }
                    String inside;
                    boolean a = Blocker.begin();
                    try {
                        inside = MpscTestSupport.carrierName();
                    } finally {
                        Blocker.end(a);
                    }
                    if (!inside.startsWith(MpscTestSupport.POOL_THREAD)) {
                        failed.set(where + "the next section did not move: " + inside);
                    }
                    if (!MpscTestSupport.carrierName().startsWith(MpscTestSupport.CARRIER)) {
                        failed.set(where + "not back on a carrier after a section: " + MpscTestSupport.carrierName());
                    }
                }
            }
        });
        assertTrue(t.join(Duration.ofMillis(DEADLINE_MS * 6)), "the thread did not finish");
        assertNull(failed.get(), failed::get);
        assertTrue(rounds.get() > 300);
        MpscTestSupport.assertRunsNow("a new thread after the rounds");
        assertTrue(MpscTestSupport.awaitPoolThreadsParked(), "a pool thread still runs: " + MpscTestSupport.poolThreads());
        assertTrue(MpscTestSupport.poolThreads().size() <= CAP, "the pool went past its bound");
    }

    private static int depthReached;

    /**
     * Makes a section at depth target, or (atEnd) ends at depth target a section begun at depth 0;
     * returns the depth reached before a StackOverflowError.
     */
    private static int edgeRound(boolean atEnd, int target) {
        depthReached = 0;
        boolean[] ended = new boolean[1];
        boolean a = atEnd ? Blocker.begin() : false;
        try {
            edgeAt(0, target, atEnd, a, ended);
        } catch (StackOverflowError e) {
            if (atEnd && !ended[0]) {
                Blocker.end(a);
            }
        }
        return depthReached;
    }

    private static void edgeAt(int d, int target, boolean atEnd, boolean a, boolean[] ended) {
        depthReached = d;
        if (d < target) {
            edgeAt(d + 1, target, atEnd, a, ended);
            return;
        }
        if (atEnd) {
            Blocker.end(a);
            ended[0] = true;
        } else {
            boolean b = Blocker.begin();
            Blocker.end(b);
        }
    }

    // ---- helpers ----

    private static void assumePool() {
        assumeTrue(MPSC && CAP > 0, "needs an MPSC offload pool");
    }
}
