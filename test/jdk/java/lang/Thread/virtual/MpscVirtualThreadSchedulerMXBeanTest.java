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
 * @summary jdk.management.VirtualThreadSchedulerMXBean on the MPSC scheduler
 * @requires vm.continuations & os.family == "linux"
 * @modules jdk.management
 * @library /test/lib
 * @run junit/othervm/native --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.pollerMode=4
 *                    -Djdk.virtualThreadScheduler.parallelism=3
 *                    MpscVirtualThreadSchedulerMXBeanTest
 */

/*
 * @test id=mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules jdk.management
 * @library /test/lib
 * @run junit/othervm/native --enable-native-access=ALL-UNNAMED
 *                    -Djdk.virtualThreadScheduler.useMpsc=true
 *                    -Djdk.pollerMode=2
 *                    -Djdk.virtualThreadScheduler.parallelism=3
 *                    MpscVirtualThreadSchedulerMXBeanTest
 */

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import jdk.management.VirtualThreadSchedulerMXBean;

import jdk.test.lib.thread.VThreadPinner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class MpscVirtualThreadSchedulerMXBeanTest {
    private static final int PARALLELISM =
            Integer.getInteger("jdk.virtualThreadScheduler.parallelism");

    private static Stream<VirtualThreadSchedulerMXBean> managedBeans() throws Exception {
        var bean1 = ManagementFactory.getPlatformMXBean(VirtualThreadSchedulerMXBean.class);
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        var bean2 = ManagementFactory.newPlatformMXBeanProxy(server,
                "jdk.management:type=VirtualThreadScheduler",
                VirtualThreadSchedulerMXBean.class);
        return Stream.of(bean1, bean2);
    }

    /**
     * Parallelism is the configured carrier count; the counts are not -1.
     */
    @ParameterizedTest
    @MethodSource("managedBeans")
    void testParallelismAndCounts(VirtualThreadSchedulerMXBean bean) {
        assertEquals(PARALLELISM, bean.getParallelism());
        assertTrue(bean.getPoolSize() >= PARALLELISM, bean.toString());
        assertTrue(bean.getMountedVirtualThreadCount() >= 0, bean.toString());
        assertTrue(bean.getQueuedVirtualThreadCount() >= 0, bean.toString());
    }

    /**
     * The carriers are fixed at start, so parallelism cannot change.
     */
    @ParameterizedTest
    @MethodSource("managedBeans")
    void testSetParallelism(VirtualThreadSchedulerMXBean bean) {
        assertThrows(UnsupportedOperationException.class,
                () -> bean.setParallelism(PARALLELISM + 1));
        assertThrows(UnsupportedOperationException.class,
                () -> bean.setParallelism(PARALLELISM));
        assertEquals(PARALLELISM, bean.getParallelism());
    }

    /**
     * Every carrier holds a pinned virtual thread blocked on a semaphore: the mounted count
     * covers them all, and threads started meanwhile stay queued.
     */
    @ParameterizedTest
    @MethodSource("managedBeans")
    void testMountedAndQueued(VirtualThreadSchedulerMXBean bean) throws Exception {
        assumeFalse(Thread.currentThread().isVirtual(), "Main thread is a virtual thread");
        // without stealing, threads started by one platform thread can share one carrier
        assumeTrue(MpscTestSupport.STEAL, "stealing is off");
        var mounted = new CountDownLatch(PARALLELISM);
        var release = new Semaphore(0);
        List<Thread> threads = new ArrayList<>();
        try {
            for (int i = 0; i < PARALLELISM; i++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    VThreadPinner.runPinned(() -> {
                        mounted.countDown();
                        release.acquireUninterruptibly();
                    });
                }));
            }
            mounted.await();
            assertTrue(bean.getMountedVirtualThreadCount() >= PARALLELISM, bean.toString());

            int queued = 5;
            var ran = new CountDownLatch(queued);
            for (int i = 0; i < queued; i++) {
                threads.add(Thread.ofVirtual().start(ran::countDown));
            }
            assertTrue(bean.getQueuedVirtualThreadCount() >= queued, bean.toString());
            assertEquals(queued, ran.getCount());
        } finally {
            release.release(PARALLELISM);
            for (Thread t : threads) {
                t.join();
            }
        }
    }
}
