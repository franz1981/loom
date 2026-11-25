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

package java.lang;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test helper patched into java.base for RunQueueTest: producers offer to one
 * RunQueue while an owner (take(true)) and thieves (take(false)) take from it.
 */
public final class RunQueueConcurrency {
    private RunQueueConcurrency() { }

    private record Item(int producer, int seq) { }

    /**
     * One run on a new queue. Returns null if every offered element was taken exactly
     * once and every consumer took each producer's elements in the order offered, else
     * what went wrong. Fails also if the run does not end within timeoutMs.
     */
    public static String run(int initialCapacity, int maxChunkCapacity, int producers,
                             int perProducer, int thieves, long timeoutMs) throws InterruptedException {
        RunQueue<Item> queue = new RunQueue<>(initialCapacity, maxChunkCapacity);
        int total = producers * perProducer;
        AtomicInteger taken = new AtomicInteger();
        List<List<Item>> takenBy = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int c = 0; c <= thieves; c++) {
            boolean owner = (c == 0);
            List<Item> mine = new ArrayList<>();
            takenBy.add(mine);
            threads.add(Thread.ofPlatform().daemon().unstarted(() -> {
                while (taken.get() < total) {
                    Item e = queue.take(owner);
                    if (e != null) {
                        mine.add(e);
                        taken.incrementAndGet();
                    } else {
                        Thread.onSpinWait();
                    }
                }
            }));
        }
        for (int p = 0; p < producers; p++) {
            int producer = p;
            threads.add(Thread.ofPlatform().daemon().unstarted(() -> {
                for (int i = 0; i < perProducer; i++) {
                    queue.offer(new Item(producer, i));
                }
            }));
        }
        threads.forEach(Thread::start);
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        for (Thread t : threads) {
            t.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())));
            if (t.isAlive()) {
                return "did not end: " + taken.get() + " of " + total + " taken";
            }
        }

        int[][] count = new int[producers][perProducer];
        for (int c = 0; c < takenBy.size(); c++) {
            int[] last = new int[producers];
            java.util.Arrays.fill(last, -1);
            for (Item e : takenBy.get(c)) {
                if (e.seq() <= last[e.producer()]) {
                    return "consumer " + c + " took " + e + " after seq " + last[e.producer()];
                }
                last[e.producer()] = e.seq();
                count[e.producer()][e.seq()]++;
            }
        }
        for (int p = 0; p < producers; p++) {
            for (int i = 0; i < perProducer; i++) {
                if (count[p][i] != 1) {
                    return new Item(p, i) + " taken " + count[p][i] + " times";
                }
            }
        }
        if (!queue.isEmpty()) {
            return "not empty after every element was taken";
        }
        return null;
    }
}
