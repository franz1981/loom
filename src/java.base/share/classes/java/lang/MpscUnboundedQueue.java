/*
 * Copyright (c) 2026, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import jdk.internal.vm.annotation.Contended;

import static java.lang.MpscVirtualThreadScheduler.STEAL;

/**
 * Multi-producer unbounded array queue using VarHandles, based on JCTools
 * MpscUnboundedArrayQueue but self-contained. Without stealing it has one consumer,
 * which calls {@link #poll()}. With {@link MpscVirtualThreadScheduler#STEAL} any number
 * of consumers call {@link #take(boolean)}: a consumer claims an element by a CAS on the
 * consumer index and then clears its slot. The two are never mixed on one queue.
 *
 * <p>Memory order: a write in release mode or stronger, read by a read in acquire mode
 * or stronger, is taken to give happens-before between them (the VarHandle access-mode
 * convention).
 *
 * <p>Invariants with stealing:
 * <ul>
 * <li>A lost claim CAS means another consumer took that index.
 * <li>A slot is written only when it is clear: an offer waits for the clear of the
 *     consumer that claimed that slot's element one lap back.
 * <li>A claim trusts a slot only below a producer index it observed.
 * </ul>
 *
 * @param <E> the type of elements held in this queue
 */
final class MpscUnboundedQueue<E> {

    private static final VarHandle PRODUCER_INDEX;
    private static final VarHandle CONSUMER_INDEX;
    private static final VarHandle PRODUCER_LIMIT;
    private static final VarHandle CONSUMER_BUFFER;
    private static final VarHandle ARRAY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            PRODUCER_INDEX = lookup.findVarHandle(MpscUnboundedQueue.class, "producerIndex", long.class);
            CONSUMER_INDEX = lookup.findVarHandle(MpscUnboundedQueue.class, "consumerIndex", long.class);
            PRODUCER_LIMIT = lookup.findVarHandle(MpscUnboundedQueue.class, "producerLimit", long.class);
            CONSUMER_BUFFER = lookup.findVarHandle(MpscUnboundedQueue.class, "consumerBuffer", Object[].class);
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Object JUMP = new Object();
    private static final Object BUFFER_CONSUMED = new Object();
    private static final int CONTINUE_TO_P_INDEX_CAS = 0;
    private static final int RETRY = 1;
    private static final int QUEUE_RESIZE = 3;

    private static final long RESIZE_BIT = 1L;

    // producer-written fields
    @Contended("producer")
    @SuppressWarnings("FieldMayBeFinal")
    private long producerIndex;
    @Contended("producer")
    @SuppressWarnings("FieldMayBeFinal")
    private long producerLimit;
    @Contended("producer")
    private long producerMask;
    @Contended("producer")
    private E[] producerBuffer;

    // consumer-written fields
    @Contended("consumer")
    @SuppressWarnings("FieldMayBeFinal")
    private long consumerIndex;
    @Contended("consumer")
    private long consumerMask;
    @Contended("consumer")
    private E[] consumerBuffer;
    // take(true) only, on the queue's carrier: a producer index it read
    @Contended("consumer")
    private long ownerProducerIndex;

    MpscUnboundedQueue(int initialCapacity) {
        if (initialCapacity < 2) {
            throw new IllegalArgumentException("Initial capacity must be 2 or more");
        }
        int p2capacity = roundToPowerOfTwo(initialCapacity);
        long mask = (p2capacity - 1L) << 1;
        @SuppressWarnings("unchecked")
        E[] buffer = (E[]) new Object[p2capacity + 1];
        producerBuffer = buffer;
        consumerBuffer = buffer;
        producerMask = mask;
        consumerMask = mask;
        soProducerLimit(mask);
    }

    private static int roundToPowerOfTwo(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Must be positive");
        }
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    private void soProducerLimit(long v) {
        PRODUCER_LIMIT.setRelease(this, v);
    }

    private long lvProducerLimit() {
        return (long) PRODUCER_LIMIT.getAcquire(this);
    }

    // A volatile load, not an acquire, as in JCTools. The carrier's pre-park re-check
    // is a volatile store of PARKED followed by a read of this index, pairing with a
    // producer's CAS here followed by a load of the carrier state; that Dekker pair
    // needs the load to be ordered after the store, which the memory model guarantees
    // for a volatile load and not for an acquire.
    private long lvProducerIndex() {
        return (long) PRODUCER_INDEX.getVolatile(this);
    }

    private boolean casProducerIndex(long expect, long newValue) {
        return PRODUCER_INDEX.compareAndSet(this, expect, newValue);
    }

    private long lvConsumerIndex() {
        return (long) CONSUMER_INDEX.getAcquire(this);
    }

    private void soConsumerIndex(long v) {
        CONSUMER_INDEX.setRelease(this, v);
    }

    private void soProducerIndex(long v) {
        PRODUCER_INDEX.setRelease(this, v);
    }

    private boolean casProducerLimit(long expect, long newValue) {
        return PRODUCER_LIMIT.compareAndSet(this, expect, newValue);
    }

    private static <E> void soRefElement(E[] buffer, int offset, E e) {
        ARRAY.setRelease(buffer, offset, e);
    }

    @SuppressWarnings("unchecked")
    private static <E> E lvRefElement(E[] buffer, int offset) {
        return (E) ARRAY.getAcquire(buffer, offset);
    }

    void offer(E e) {
        if (null == e) {
            throw new NullPointerException();
        }

        long mask;
        E[] buffer;
        long pIndex;

        while (true) {
            long producerLimit = lvProducerLimit();
            pIndex = lvProducerIndex();
            if ((pIndex & RESIZE_BIT) == 1) {
                continue;
            }

            mask = this.producerMask;
            buffer = this.producerBuffer;

            // With stealing, the consumer that took this slot's element one lap back clears it
            // after its claim: never write over it, or that clear erases this element. Keep the
            // check before the limit test: it also guards the slot that receives JUMP on a resize,
            // which is what lets take count an index with the resize bit set as one more element.
            if (STEAL && lvRefElement(buffer, modifiedCalcCircularRefElementOffset(pIndex, mask)) != null) {
                if (lvProducerIndex() == pIndex) {
                    Thread.onSpinWait();      // that consumer is between its claim and its clear
                }
                continue;                     // else pIndex was stale: retry at once
            }

            if (producerLimit <= pIndex) {
                int result = offerSlowPath(mask, pIndex, producerLimit);
                switch (result) {
                    case CONTINUE_TO_P_INDEX_CAS:
                        break;
                    case RETRY:
                        continue;
                    case QUEUE_RESIZE:
                        resize(mask, buffer, pIndex, e);
                        return;
                }
            }

            if (casProducerIndex(pIndex, pIndex + 2)) {
                break;
            }
        }
        final int offset = modifiedCalcCircularRefElementOffset(pIndex, mask);
        soRefElement(buffer, offset, e);
    }

    private int offerSlowPath(long mask, long pIndex, long producerLimit) {
        final long cIndex = lvConsumerIndex();
        long bufferCapacity = mask;
        if (cIndex + bufferCapacity > pIndex) {
            if (!casProducerLimit(producerLimit, cIndex + bufferCapacity)) {
                return RETRY;
            }
            return CONTINUE_TO_P_INDEX_CAS;
        }
        if (casProducerIndex(pIndex, pIndex + 1)) {
            return QUEUE_RESIZE;
        }
        return RETRY;
    }

    private void resize(long oldMask, E[] oldBuffer, long pIndex, final E e) {
        int newBufferLength = oldBuffer.length;
        final E[] newBuffer;
        try {
            @SuppressWarnings("unchecked")
            E[] b = (E[]) new Object[newBufferLength];
            newBuffer = b;
        } catch (OutOfMemoryError oome) {
            // only the resizer changes the producer index while the resize bit is set:
            // clear it, or every later offer waits for a resize that never ends
            soProducerIndex(pIndex);
            throw oome;
        }

        producerBuffer = newBuffer;
        final int newMask = (newBufferLength - 2) << 1;
        producerMask = newMask;

        final int offsetInOld = modifiedCalcCircularRefElementOffset(pIndex, oldMask);
        final int offsetInNew = modifiedCalcCircularRefElementOffset(pIndex, newMask);

        soRefElement(newBuffer, offsetInNew, e);
        soRefElement(oldBuffer, nextArrayOffset(oldMask), newBuffer);

        final long cIndex = lvConsumerIndex();
        final long availableInQueue = Integer.MAX_VALUE - (pIndex - cIndex);
        if (availableInQueue <= 0) {
            throw new IllegalStateException();
        }

        soProducerLimit(pIndex + Math.min(newMask, availableInQueue));
        soProducerIndex(pIndex + 2);
        soRefElement(oldBuffer, offsetInOld, JUMP);
    }

    private int nextArrayOffset(final long mask) {
        return modifiedCalcCircularRefElementOffset(mask + 2, Long.MAX_VALUE);
    }

    private static int modifiedCalcCircularRefElementOffset(long index, long mask) {
        return (int) ((index & mask) >> 1);
    }

    @SuppressWarnings("unchecked")
    E poll() {
        final E[] buffer = consumerBuffer;
        final long index = consumerIndex;
        final long mask = consumerMask;

        final int offset = modifiedCalcCircularRefElementOffset(index, mask);
        Object e;
        // re-read while waiting: a resize that runs out of memory rolls the producer index back
        while ((e = lvRefElement(buffer, offset)) == null) {
            long pIndex = lvProducerIndex();
            pIndex += (pIndex & RESIZE_BIT);
            if (index == pIndex) {
                return null;
            }
            Thread.onSpinWait();
        }
        if (e == JUMP) {
            final E[] nextBuffer = nextBuffer(buffer, mask);
            return newBufferPoll(nextBuffer, index);
        }
        soRefElement(buffer, offset, null);
        soConsumerIndex(index + 2);
        return (E) e;
    }

    private E[] nextBuffer(final E[] buffer, final long mask) {
        final int nextArrayOffset = nextArrayOffset(mask);
        @SuppressWarnings("unchecked")
        final E[] nextBuffer = (E[]) lvRefElement(buffer, nextArrayOffset);
        consumerBuffer = nextBuffer;
        consumerMask = (nextBuffer.length - 2L) << 1;
        soRefElement(buffer, nextArrayOffset, BUFFER_CONSUMED);
        return nextBuffer;
    }

    /**
     * With stealing: takes the element at the consumer index, racing other consumers.
     * Returns null only if the queue was empty at a read made during this call; waits
     * for a producer whose index claim is done and whose store is not. owner: the caller
     * is this queue's carrier.
     */
    @SuppressWarnings("unchecked")
    E take(boolean owner) {
        for (;;) {
            long index = (long) CONSUMER_INDEX.getVolatile(this);
            long pIndex = owner ? ownerProducerIndex : 0L;
            if (index >= pIndex) {
                pIndex = lvProducerIndex();
                pIndex += (pIndex & RESIZE_BIT);
                if (index >= pIndex) {
                    return null;
                }
                if (owner) {
                    ownerProducerIndex = pIndex;
                }
            }
            E[] buffer = (E[]) CONSUMER_BUFFER.getAcquire(this);
            long mask = (buffer.length - 2L) << 1;
            int offset = modifiedCalcCircularRefElementOffset(index, mask);
            Object e = lvRefElement(buffer, offset);
            if (e == null || e == JUMP) {
                // another consumer took index; this re-read also stops a consumer whose index is
                // a lap stale from moving consumerBuffer on the next lap's JUMP, a lap early
                if ((long) CONSUMER_INDEX.getVolatile(this) != index) {
                    continue;
                }
                if (e == null) {
                    if (owner) {
                        ownerProducerIndex = index;    // read the producer index again: an offer can roll back
                    }
                    Thread.onSpinWait();               // its producer has claimed it and not stored it yet
                } else {
                    int link = nextArrayOffset(mask);
                    Object next = lvRefElement(buffer, link);
                    if (next != BUFFER_CONSUMED && CONSUMER_BUFFER.compareAndSet(this, buffer, (E[]) next)) {
                        soRefElement(buffer, link, BUFFER_CONSUMED);
                    }
                }
                continue;
            }
            if (CONSUMER_INDEX.compareAndSet(this, index, index + 2)) {
                soRefElement(buffer, offset, null);    // the producer one lap on waits for this
                return (E) e;
            }
        }
    }

    private E newBufferPoll(E[] nextBuffer, final long index) {
        final int offset = modifiedCalcCircularRefElementOffset(index, consumerMask);
        final E n = lvRefElement(nextBuffer, offset);
        if (n == null) {
            throw new IllegalStateException("new buffer must have at least one element");
        }
        soRefElement(nextBuffer, offset, null);
        soConsumerIndex(index + 2);
        return n;
    }

    boolean isEmpty() {
        long cIndex = lvConsumerIndex();
        long pIndex = lvProducerIndex();
        pIndex += (pIndex & RESIZE_BIT);
        return cIndex == pIndex;
    }

    /**
     * An estimate of the number of queued elements, >= 0: one load of each index in
     * isEmpty()'s order, no retry while the consumer index moves.
     * It is 0 exactly when isEmpty() would be true at the same two reads.
     */
    int sizeEstimate() {
        long cIndex = lvConsumerIndex();
        long pIndex = lvProducerIndex();
        pIndex += (pIndex & RESIZE_BIT);
        long size = (pIndex - cIndex) >> 1;
        return size <= 0 ? 0 : (int) Math.min(size, Integer.MAX_VALUE);
    }
}
