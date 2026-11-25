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
 * @summary MPSC scheduler under a custom default scheduler that delegates to it: its threads are
 *          not moved to the offload pool
 * @requires vm.continuations
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm -esa -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.implClass=MpscOffloadCustomSchedulerTest$Delegating
 *                    MpscOffloadCustomSchedulerTest
 */

/*
 * @test id=mpsc-mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run junit/othervm -esa -Djdk.virtualThreadScheduler.useMpsc=true -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=1
 *                    -Djdk.virtualThreadScheduler.maxPoolSize=17
 *                    -Djdk.virtualThreadScheduler.implClass=MpscOffloadCustomSchedulerTest$Delegating
 *                    MpscOffloadCustomSchedulerTest
 */

import java.lang.Thread.VirtualThreadScheduler;
import java.lang.Thread.VirtualThreadTask;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import jdk.internal.misc.Blocker;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MpscOffloadCustomSchedulerTest {

    /** The default scheduler: counts each thread's starts and continuations, then delegates. */
    public static class Delegating implements VirtualThreadScheduler {
        static final Set<Thread> STARTED = ConcurrentHashMap.newKeySet();
        static final AtomicInteger CONTINUED = new AtomicInteger();
        private final VirtualThreadScheduler builtin;

        public Delegating(VirtualThreadScheduler builtin) {
            this.builtin = builtin;
        }

        @Override
        public void onStart(VirtualThreadTask task) {
            STARTED.add(task.thread());
            builtin.onStart(task);
        }

        @Override
        public void onContinue(VirtualThreadTask task) {
            CONTINUED.incrementAndGet();
            builtin.onContinue(task);
        }
    }

    /**
     * 100 sections with a yield inside complete on an MPSC carrier; no pool thread starts and the
     * custom scheduler sees every yield.
     */
    @Test
    void sectionsOfACustomThreadStayOnTheCarrier() throws Exception {
        AtomicReference<String> where = new AtomicReference<>();
        AtomicReference<Throwable> failed = new AtomicReference<>();
        int continued0 = Delegating.CONTINUED.get();
        Thread t = Thread.ofVirtual().start(() -> {
            try {
                for (int i = 0; i < 100; i++) {
                    boolean a = Blocker.begin();
                    try {
                        if (!MpscTestSupport.carrierName().startsWith(MpscTestSupport.CARRIER)) {
                            where.set(MpscTestSupport.carrierName());
                        }
                        Thread.yield();
                    } finally {
                        Blocker.end(a);
                    }
                }
            } catch (Throwable e) {
                failed.set(e);
            }
        });
        assertTrue(t.join(Duration.ofMillis(MpscTestSupport.DEADLINE_MS)), "the sections did not complete");
        assertNull(failed.get(), () -> "a section failed: " + failed.get());
        assertTrue(Delegating.STARTED.contains(t), "the thread is not the custom scheduler's: the test proved nothing");
        assertNull(where.get(), () -> "a section of a custom scheduler's thread ran on " + where.get());
        assertTrue(Delegating.CONTINUED.get() - continued0 >= 100, "the custom scheduler missed continuations");
        assertEquals(List.of(), MpscTestSupport.poolThreads(), "pool threads started");
    }
}
