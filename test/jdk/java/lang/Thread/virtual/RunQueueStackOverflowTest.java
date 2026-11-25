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
 * @test id=plain
 * @summary A StackOverflowError between a run queue offer's claim and its store leaves the
 *          queue usable and in order, on a queue that never resizes and one that always does;
 *          the plain -Xint id skips if no overflow fell between a claim and its store
 * @library /test/lib
 * @compile/module=java.base java/lang/RunQueueOverflow.java
 * @run main/othervm RunQueueStackOverflowTest plain
 */

/*
 * @test id=plain-xint
 * @library /test/lib
 * @compile/module=java.base java/lang/RunQueueOverflow.java
 * @run main/othervm -Xint RunQueueStackOverflowTest plain window
 */

/*
 * @test id=resize
 * @library /test/lib
 * @compile/module=java.base java/lang/RunQueueOverflow.java
 * @run main/othervm RunQueueStackOverflowTest resize
 */

/*
 * @test id=resize-xint
 * @library /test/lib
 * @compile/module=java.base java/lang/RunQueueOverflow.java
 * @run main/othervm -Xint RunQueueStackOverflowTest resize
 */

import java.lang.RunQueueOverflow;

import jdk.test.lib.Utils;
import jtreg.SkippedException;

public class RunQueueStackOverflowTest {
    private static final int QUEUE_ROUNDS = 64;
    private static final int MAX_OFFERS = 1 << 17;
    private static final long STACK_SIZE = 512 * 1024;
    private static final long TIMEOUT_MS = Utils.adjustTimeout(10_000);

    public static void main(String[] args) throws Exception {
        boolean window = args.length > 1 && args[1].equals("window");
        switch (args[0]) {
            case "plain" -> queueRounds(args[0], MAX_OFFERS, MAX_OFFERS, window);
            // capacity 2 and no consumer: every offer but the first starts a chunk
            case "resize" -> queueRounds(args[0], 2, 2, window);
            default -> throw new IllegalArgumentException(args[0]);
        }
    }

    private static void queueRounds(String kind, int initialCapacity, int maxChunkCapacity,
                                    boolean window) throws Exception {
        int queuedByAThrowingOffer = 0;
        int minReturned = Integer.MAX_VALUE, maxReturned = 0;
        for (int round = 0; round < QUEUE_ROUNDS; round++) {
            RunQueueOverflow.Result r = RunQueueOverflow.round(
                    initialCapacity, maxChunkCapacity, MAX_OFFERS, round, STACK_SIZE, TIMEOUT_MS);
            if (r.failure() != null) {
                throw new RuntimeException(kind + ", round " + round + ": " + r.failure());
            }
            if (r.queuedByAThrowingOffer()) {
                queuedByAThrowingOffer++;
            }
            minReturned = Math.min(minReturned, r.returned());
            maxReturned = Math.max(maxReturned, r.returned());
        }
        System.out.println(kind + ": " + QUEUE_ROUNDS + " rounds, offers per round " + minReturned + ".."
                + maxReturned + ", rounds whose throwing offer had queued its element: " + queuedByAThrowingOffer);
        if (window && queuedByAThrowingOffer == 0) {
            throw new SkippedException(kind + ": no round overflowed between a claim and its store");
        }
    }
}
