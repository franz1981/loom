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
 * Multi-producer unbounded queue of linked array chunks using VarHandles, based on JCTools
 * MpscUnboundedArrayQueue and MpscGrowableArrayQueue but self-contained. Without stealing it has
 * one consumer, which calls {@link #poll()}. With {@link MpscVirtualThreadScheduler#STEAL} any
 * number of consumers call {@link #take(boolean)}: a consumer claims an element by a CAS on the
 * consumer index and then clears its slot. The two are never mixed on one queue.
 *
 * <p>A chunk holds its ring slots, the link to the next chunk, and, with stealing, its first
 * index as a Long (none in the first chunk).
 *
 * <p>The first chunk has the initial capacity. A full ring doubles the next chunk's capacity up to
 * the maximum chunk capacity; an offer that meets a held slot starts a chunk of the same length.
 * Chunk lengths never decrease along the chain, so a producer limit read in an earlier chunk is
 * still valid in a later one. In a shorter chunk, a producer holding a limit read in a longer one
 * could write a slot whose claimant has not stored yet, and that store would then overwrite it.
 * The maximum chunk capacity bounds chunks, not elements: the queue holds fewer than about 2^30
 * elements, and an offer beyond that throws IllegalStateException.
 *
 * <p>Memory order: a write in release mode or stronger, read by a read in acquire mode
 * or stronger, is taken to give happens-before between them (the VarHandle access-mode
 * convention).
 *
 * <p>Invariants with stealing:
 * <ul>
 * <li>A lost claim CAS means another consumer took that index.
 * <li>A slot is written only when it is clear: an offer whose slot still holds an element
 *     starts a new chunk at its index instead. No JUMP is written.
 * <li>An index belongs to the chunk whose start is at or below it and whose successor's start
 *     is above it.
 * <li>A consumer reads a slot only below the smaller of a producer index it read, without the
 *     resize bit, and the start of the chunk's successor.
 * <li>A producer waits only for another producer's resize. A consumer waits only for a producer
 *     between its index CAS and its store, or for a resizer.
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
    private E[] producerBuffer;

    // consumer-written fields
    @Contended("consumer")
    @SuppressWarnings("FieldMayBeFinal")
    private long consumerIndex;
    @Contended("consumer")
    private long consumerMask;
    @Contended("consumer")
    private E[] consumerBuffer;
    // take(true) only, on the queue's carrier: below this index the owner reads slots without
    // reading the producer index (see the class doc)
    @Contended("consumer")
    private long ownerTrusted;

    // the largest maximum chunk capacity: a chunk's mask, (capacity - 1) << 1, must fit an int
    static final int MAX_CHUNK_CAPACITY = 1 << 30;

    // a full ring doubles the chunk capacity up to this, a power of 2
    private final int maxChunkCapacity;

    MpscUnboundedQueue(int initialCapacity, int maxChunkCapacity) {
        if (initialCapacity < 2) {
            throw new IllegalArgumentException("Initial capacity must be 2 or more");
        }
        if (maxChunkCapacity < initialCapacity || maxChunkCapacity > MAX_CHUNK_CAPACITY) {
            throw new IllegalArgumentException("Maximum chunk capacity must be in [initial capacity, 2^30]");
        }
        int p2capacity = roundToPowerOfTwo(initialCapacity);
        this.maxChunkCapacity = roundToPowerOfTwo(maxChunkCapacity);
        long mask = (p2capacity - 1L) << 1;
        @SuppressWarnings("unchecked")
        E[] buffer = (E[]) new Object[p2capacity + 2];
        producerBuffer = buffer;
        consumerBuffer = buffer;
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
                Thread.onSpinWait();          // another producer is inside its resize
                continue;
            }

            buffer = this.producerBuffer;
            // the mask of this buffer: a separately read mask can belong to a chunk of another
            // length, and the held-slot read below comes before the CAS that would reject the pair
            mask = (buffer.length - 3L) << 1;

            // With stealing a consumer clears its slot after its claim, so the element one lap back
            // may still be here, and that late clear would erase anything stored now: this index
            // starts a new chunk instead. Every path to the index CAS below passes this check.
            if (STEAL && lvRefElement(buffer, modifiedCalcCircularRefElementOffset(pIndex, mask)) != null) {
                if (casProducerIndex(pIndex, pIndex + 1)) {
                    resize(mask, buffer, pIndex, e, false);
                    return;
                }
                continue;
            }

            if (producerLimit <= pIndex) {
                int result = offerSlowPath(mask, pIndex, producerLimit);
                switch (result) {
                    case CONTINUE_TO_P_INDEX_CAS:
                        break;
                    case RETRY:
                        continue;
                    case QUEUE_RESIZE:
                        resize(mask, buffer, pIndex, e, true);
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

    private void resize(long oldMask, E[] oldBuffer, long pIndex, final E e, boolean grow) {
        final long availableInQueue = Integer.MAX_VALUE - (pIndex - lvConsumerIndex());
        if (availableInQueue <= 0) {
            soProducerIndex(pIndex);        // as on out of memory: the resize bit must not stay set
            throw new IllegalStateException();
        }

        int capacity = oldBuffer.length - 2;
        // a held slot keeps the length: a stalled claimant costs a chunk of this length
        if (grow && capacity < maxChunkCapacity) {
            capacity <<= 1;
        }
        int newBufferLength = capacity + 2;
        final E[] newBuffer;
        try {
            @SuppressWarnings("unchecked")
            E[] b = (E[]) new Object[newBufferLength];
            if (STEAL) {
                ((Object[]) b)[startOffset(b)] = Long.valueOf(pIndex);   // published by the link's release store
            }
            newBuffer = b;
        } catch (OutOfMemoryError oome) {
            // only the resizer changes the producer index while the resize bit is set:
            // clear it, or every later offer waits for a resize that never ends
            soProducerIndex(pIndex);
            throw oome;
        }

        producerBuffer = newBuffer;
        final int newMask = (newBufferLength - 3) << 1;

        final int offsetInNew = modifiedCalcCircularRefElementOffset(pIndex, newMask);

        soRefElement(newBuffer, offsetInNew, e);
        soRefElement(oldBuffer, nextArrayOffset(oldMask), newBuffer);

        soProducerLimit(pIndex + Math.min(newMask, availableInQueue));
        soProducerIndex(pIndex + 2);
        if (!STEAL) {
            // with stealing the slot at pIndex may still hold a claimed element: consumers find
            // the new chunk by its start instead
            soRefElement(oldBuffer, modifiedCalcCircularRefElementOffset(pIndex, oldMask), JUMP);
        }
    }

    private int nextArrayOffset(final long mask) {
        return modifiedCalcCircularRefElementOffset(mask + 2, Long.MAX_VALUE);
    }

    private static int startOffset(Object[] buffer) {
        return buffer.length - 1;
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
        consumerMask = (nextBuffer.length - 3L) << 1;
        soRefElement(buffer, nextArrayOffset, BUFFER_CONSUMED);
        return nextBuffer;
    }

    /**
     * With stealing: takes the element at the consumer index, racing other consumers.
     * Returns null only if the queue was empty at a read made during this call. Waits for a
     * producer between its index claim and its store, and for a resizer that holds the index.
     * owner: the caller is this queue's carrier.
     */
    @SuppressWarnings("unchecked")
    E take(boolean owner) {
        for (;;) {
            // the index before the buffer: the buffer read is then the index's chunk or a later one
            long index = (long) CONSUMER_INDEX.getVolatile(this);
            E[] buffer = (E[]) CONSUMER_BUFFER.getAcquire(this);
            long trusted = owner ? ownerTrusted : 0L;
            if (index >= trusted) {
                // the producer index before the link: an unlinked chunk then holds every index below it
                long pIndex = lvProducerIndex();
                if (index >= pIndex + (pIndex & RESIZE_BIT)) {
                    return null;
                }
                trusted = pIndex & ~RESIZE_BIT;
                int link = nextArrayOffset((buffer.length - 3L) << 1);
                Object next = lvRefElement(buffer, link);
                if (next == BUFFER_CONSUMED) {
                    continue;
                }
                if (next != null) {
                    long start = (Long) lvRefElement((Object[]) next, startOffset((Object[]) next));
                    if (index >= start) {
                        if (CONSUMER_BUFFER.compareAndSet(this, buffer, (E[]) next)) {
                            soRefElement(buffer, link, BUFFER_CONSUMED);
                        }
                        continue;
                    }
                    trusted = Math.min(trusted, start);
                }
                if (index >= trusted) {
                    Thread.onSpinWait();               // a resizer holds index
                    continue;
                }
                if (owner) {
                    ownerTrusted = trusted;
                }
            }
            long mask = (buffer.length - 3L) << 1;
            int offset = modifiedCalcCircularRefElementOffset(index, mask);
            Object e = lvRefElement(buffer, offset);
            if (e == null) {
                if ((long) CONSUMER_INDEX.getVolatile(this) == index) {
                    Thread.onSpinWait();               // its producer has claimed index and not stored it yet
                }
                continue;
            }
            if (CONSUMER_INDEX.compareAndSet(this, index, index + 2)) {
                soRefElement(buffer, offset, null);
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
