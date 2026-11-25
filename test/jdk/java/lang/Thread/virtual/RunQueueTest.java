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
 * @test
 * @summary MPSC scheduler's queue: with concurrent producers, owner and thieves, every element
 *          is taken once and each consumer takes each producer's elements in offer order
 * @library /test/lib
 * @compile/module=java.base java/lang/RunQueueConcurrency.java
 * @run junit/othervm RunQueueTest
 */

import java.lang.RunQueueConcurrency;

import jdk.test.lib.Utils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

class RunQueueTest {
    private static final long TIMEOUT_MS = Utils.adjustTimeout(60_000);

    @ParameterizedTest
    @CsvSource({
        // initialCapacity, maxChunkCapacity, producers, perProducer, thieves
        "2, 2, 4, 20000, 2",
        "2, 64, 4, 50000, 3",
        "64, 1073741824, 4, 50000, 3",
        "64, 1073741824, 1, 100000, 1",
    })
    void testConcurrentProducersAndConsumers(int initialCapacity, int maxChunkCapacity,
                                             int producers, int perProducer, int thieves)
            throws Exception {
        for (int i = 0; i < 20; i++) {
            int run = i;
            String failure = RunQueueConcurrency.run(initialCapacity, maxChunkCapacity,
                    producers, perProducer, thieves, TIMEOUT_MS);
            assertNull(failure, () -> "run " + run + ": " + failure);
        }
    }
}
