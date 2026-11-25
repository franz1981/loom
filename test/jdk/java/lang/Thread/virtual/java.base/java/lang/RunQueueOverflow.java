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

import java.util.concurrent.TimeUnit;

/**
 * Test helper patched into java.base for RunQueueStackOverflowTest: a thread offers to
 * a RunQueue at every stack depth until StackOverflowError, then the queue must
 * still take an offer and give back, in order, everything the thread queued.
 */
public final class RunQueueOverflow {
    private static final Object MARK = new Object();

    private final RunQueue<Object> queue;
    private final Object[] elements;
    // offers that returned normally
    private int returned;
    private boolean overflowed;

    private RunQueueOverflow(int initialCapacity, int maxChunkCapacity, int maxOffers) {
        queue = new RunQueue<>(initialCapacity, maxChunkCapacity);
        elements = new Object[maxOffers];
        for (int i = 0; i < maxOffers; i++) {
            elements[i] = new Object();
        }
    }

    private void offerAtEveryDepth() {
        if (returned == elements.length) {
            return;
        }
        queue.offer(elements[returned]);
        returned++;
        offerAtEveryDepth();
    }

    private void pad(int frames) {
        if (frames > 0) {
            pad(frames - 1);
        } else {
            offerAtEveryDepth();
        }
    }

    /** The result of a round: {@code failure} is null if the queue stayed consistent. */
    public record Result(String failure, int returned, boolean queuedByAThrowingOffer) { }

    /**
     * One round on a new queue: a producer thread with the given stack size first recurses
     * {@code pad} frames, then offers at every depth until StackOverflowError. Then another
     * thread's offer of a mark must return, and a consumer must take the producer's elements
     * in order and the mark, each within timeoutMs. An offer that threw may have queued its
     * element; nothing else may differ.
     */
    public static Result round(int initialCapacity, int maxChunkCapacity, int maxOffers,
                               int pad, long stackSize, long timeoutMs) throws Exception {
        RunQueueOverflow r = new RunQueueOverflow(initialCapacity, maxChunkCapacity, maxOffers);
        Thread producer = new Thread(null, () -> {
            try {
                r.pad(pad);
            } catch (StackOverflowError e) {
                r.overflowed = true;
            }
        }, "overflowing-producer", stackSize);
        producer.setDaemon(true);
        producer.start();
        producer.join(timeoutMs);
        if (producer.isAlive()) {
            return r.failure("the overflowing producer did not end");
        }
        if (!r.overflowed) {
            return r.failure("no StackOverflowError after " + r.returned + " offers: raise maxOffers");
        }

        Thread marker = Thread.ofPlatform().daemon().start(() -> r.queue.offer(MARK));
        marker.join(timeoutMs);
        if (marker.isAlive()) {
            return r.failure("an offer after the overflow did not return");
        }

        Object[] taken = new Object[r.returned + 2];
        int[] count = new int[1];
        boolean[] marked = new boolean[1];
        Thread consumer = Thread.ofPlatform().daemon().start(() -> {
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (System.nanoTime() < end) {
                Object e = r.queue.take(true);
                if (e == MARK) {
                    marked[0] = true;
                    return;
                }
                if (e != null) {
                    if (count[0] == taken.length) {
                        return;
                    }
                    taken[count[0]++] = e;
                }
            }
        });
        consumer.join(timeoutMs * 2);
        if (consumer.isAlive()) {
            return r.failure("the consumer did not return after " + count[0] + " elements");
        }
        if (!marked[0]) {
            return r.failure("the mark was not taken after " + count[0] + " elements");
        }
        int n = count[0];
        if (n != r.returned && n != r.returned + 1) {
            return r.failure(n + " elements taken");
        }
        for (int i = 0; i < n; i++) {
            if (taken[i] != r.elements[i]) {
                return r.failure("element " + i + " out of order");
            }
        }
        if (!r.queue.isEmpty()) {
            return r.failure("not empty after the mark");
        }
        return new Result(null, r.returned, n == r.returned + 1);
    }

    private Result failure(String what) {
        return new Result(what + "; returned=" + returned, returned, false);
    }
}
