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

/**
 * The run queue of one carrier: multi-producer, multi-consumer, unbounded, of linked
 * array chunks, based on JCTools MpscUnboundedArrayQueue and MpscGrowableArrayQueue but
 * self-contained. Any thread offers. The owner takes with {@code take(true)}, a fast
 * path that keeps the producer bound it last read; thieves take with
 * {@code take(false)}. A consumer claims an element by a CAS on the consumer index and
 * then clears its slot. It holds fewer than about 2^30 elements.
 *
 * <p>A chunk holds its ring slots, the link to the next chunk, and its first index as a
 * Long (none in the first chunk). The first chunk has the initial capacity. A full ring
 * doubles the next chunk's capacity up to the maximum chunk capacity; an offer that meets
 * a held slot starts a chunk of the same length. An offer beyond about 2^30 elements
 * throws IllegalStateException.
 *
 * <p>Invariants:
 * <ul>
 * <li>Chunk lengths never decrease along the chain, so a producer limit read in an
 *     earlier chunk is still valid in a later one.
 * <li>A lost claim CAS means another consumer took that index.
 * <li>A slot is written only when it is clear: an offer whose slot still holds an element
 *     starts a new chunk at its index instead.
 * <li>An index belongs to the chunk whose start is at or below it and whose successor's
 *     start is above it.
 * <li>A consumer reads a slot only below the smaller of a producer index it read,
 *     without the resize bit, and the start of the chunk's successor. The owner
 *     (take(true)) keeps that bound across takes, as ownerTrusted, and reads the
 *     producer index again only at it.
 * <li>A producer waits only for another producer's resize. A consumer waits only for a
 *     producer between its index CAS and its store, or for a resizer.
 * <li>Every claimed index is completed even if its claimant throws (a StackOverflowError
 *     can hit any call; ReservedStackAccess does not apply on a virtual thread), else the
 *     waits above never end: no call comes between a claim and the try that guards it,
 *     and the handlers finish with plain stores and the volatile index store, then
 *     rethrow.
 * </ul>
 *
 * <p>Memory order: an element's release store pairs with the consumer's acquire load of
 * its slot; a new chunk, with its start index, is published by the release store of the
 * link and read by an acquire load of it.
 *
 * <p>Asynchronous exceptions (JVMTI StopThread) are not handled: one delivered after a
 * call's store makes its handler repeat that store.
 *
 * @param <E> the type of elements held in this queue
 */
final class RunQueue<E> {

    private static final VarHandle PRODUCER_INDEX;
    private static final VarHandle CONSUMER_INDEX;
    private static final VarHandle PRODUCER_LIMIT;
    private static final VarHandle CONSUMER_BUFFER;
    private static final VarHandle ARRAY;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> c = RunQueue.class;
            PRODUCER_INDEX = lookup.findVarHandle(c, "producerIndex", long.class);
            CONSUMER_INDEX = lookup.findVarHandle(c, "consumerIndex", long.class);
            PRODUCER_LIMIT = lookup.findVarHandle(c, "producerLimit", long.class);
            CONSUMER_BUFFER = lookup.findVarHandle(c, "consumerBuffer", Object[].class);
            ARRAY = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static final Object BUFFER_CONSUMED = new Object();
    private static final int CONTINUE_TO_P_INDEX_CAS = 0;
    private static final int RETRY = 1;
    private static final int QUEUE_RESIZE = 2;

    private static final long RESIZE_BIT = 1L;

    // producer-written; volatile for the plain writes of the error paths, every other
    // access goes through a VarHandle with its own mode
    @Contended("producer")
    private volatile long producerIndex;
    @Contended("producer")
    private volatile long producerLimit;
    @Contended("producer")
    private E[] producerBuffer;

    // consumer-written fields
    @Contended("consumer")
    private long consumerIndex;
    @Contended("consumer")
    private E[] consumerBuffer;
    @Contended("consumer")
    private long ownerTrusted;

    // the largest maximum chunk capacity: a chunk's mask, (capacity - 1) << 1, must fit an int
    static final int MAX_CHUNK_CAPACITY = 1 << 30;

    private final int maxChunkCapacity;

    RunQueue(int initialCapacity, int maxChunkCapacity) {
        if (initialCapacity < 2) {
            throw new IllegalArgumentException("Initial capacity must be 2 or more");
        }
        if (maxChunkCapacity < initialCapacity || maxChunkCapacity > MAX_CHUNK_CAPACITY) {
            throw new IllegalArgumentException(
                    "Maximum chunk capacity must be in [initial capacity, 2^30]");
        }
        int p2capacity = roundToPowerOfTwo(initialCapacity);
        this.maxChunkCapacity = roundToPowerOfTwo(maxChunkCapacity);
        long mask = (p2capacity - 1L) << 1;
        @SuppressWarnings("unchecked")
        E[] buffer = (E[]) new Object[p2capacity + 2];
        producerBuffer = buffer;
        consumerBuffer = buffer;
        soProducerLimit(mask);
    }

    private static int roundToPowerOfTwo(int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    private void soProducerLimit(long v) {
        PRODUCER_LIMIT.setRelease(this, v);
    }

    private long lvProducerLimit() {
        return (long) PRODUCER_LIMIT.getAcquire(this);
    }

    // volatile, not acquire: MpscCarrierThread's park recheck needs this load ordered
    // after its volatile store of PARKED
    private long lvProducerIndex() {
        return (long) PRODUCER_INDEX.getVolatile(this);
    }

    private boolean casProducerIndex(long expect, long newValue) {
        return PRODUCER_INDEX.compareAndSet(this, expect, newValue);
    }

    private long lvConsumerIndex() {
        return (long) CONSUMER_INDEX.getAcquire(this);
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
            if ((pIndex & RESIZE_BIT) != 0) {
                Thread.onSpinWait();          // another producer is inside its resize
                continue;
            }

            buffer = this.producerBuffer;
            // from this buffer: a separately read mask may belong to another chunk, and
            // the held-slot read below comes before the CAS that would reject the pair
            mask = (buffer.length - 3L) << 1;

            // a consumer clears its slot after its claim: the element one lap back may
            // still be here, and its late clear would erase a store now
            if (lvRefElement(buffer, modifiedCalcCircularRefElementOffset(pIndex, mask)) != null) {
                if (resize(mask, buffer, pIndex, e, false)) {
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
                        if (resize(mask, buffer, pIndex, e, true)) {
                            return;
                        }
                        continue;
                }
            }

            if (casProducerIndex(pIndex, pIndex + 2)) {
                break;
            }
        }
        // modifiedCalcCircularRefElementOffset inline: no call between the claim and the try
        final int offset = (int) ((pIndex & mask) >> 1);
        try {
            soRefElement(buffer, offset, e);
        } catch (Throwable t) {
            buffer[offset] = e;
            throw t;
        }
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
        return QUEUE_RESIZE;
    }

    /**
     * Claims pIndex with the resize bit and starts a chunk there holding e. False if another
     * producer claimed pIndex first.
     */
    private boolean resize(long oldMask, E[] oldBuffer, long pIndex, final E e, boolean grow) {
        int capacity = oldBuffer.length - 2;
        if (grow && capacity < maxChunkCapacity) {
            capacity <<= 1;
        }
        final int newBufferLength = capacity + 2;
        final int newMask = (newBufferLength - 3) << 1;
        final int offsetInNew = modifiedCalcCircularRefElementOffset(pIndex, newMask);
        final int link = nextArrayOffset(oldMask);
        if (!casProducerIndex(pIndex, pIndex + 1)) {
            return false;
        }
        // only this producer changes the producer index until it clears the resize bit
        E[] newBuffer = null;
        long limit = 0;
        boolean linked = false;
        boolean committed = false;
        try {
            final long availableInQueue = Integer.MAX_VALUE - (pIndex - lvConsumerIndex());
            if (availableInQueue <= 0) {
                throw new IllegalStateException("queue is full");
            }
            limit = pIndex + Math.min(newMask, availableInQueue);
            @SuppressWarnings("unchecked")
            E[] b = (E[]) new Object[newBufferLength];
            // published by the link's release store
            ((Object[]) b)[startOffset(b)] = Long.valueOf(pIndex);
            newBuffer = b;
            soRefElement(newBuffer, offsetInNew, e);
            soRefElement(oldBuffer, link, newBuffer);
            linked = true;
            producerBuffer = newBuffer;
            soProducerLimit(limit);
            soProducerIndex(pIndex + 2);
            committed = true;
        } catch (Throwable t) {
            if (!linked) {
                // nothing is published: give the index back
                producerIndex = pIndex;
            } else if (!committed) {
                producerBuffer = newBuffer;
                producerLimit = limit;
                producerIndex = pIndex + 2;
            }
            throw t;
        }
        return true;
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

    /**
     * Takes the element at the consumer index, racing other consumers; null only if the
     * queue was empty at a read made during this call. owner: the caller is this queue's
     * carrier.
     */
    @SuppressWarnings("unchecked")
    E take(boolean owner) {
        for (;;) {
            // the index before the buffer: the buffer read is then the index's chunk or a later one
            long index = (long) CONSUMER_INDEX.getVolatile(this);
            E[] buffer = (E[]) CONSUMER_BUFFER.getAcquire(this);
            long trusted = owner ? ownerTrusted : 0L;
            if (index >= trusted) {
                // the producer index before the link: an unlinked chunk then holds every index
                // below it
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
                    // its producer has claimed index and not stored it yet
                    Thread.onSpinWait();
                }
                continue;
            }
            if (CONSUMER_INDEX.compareAndSet(this, index, index + 2)) {
                soRefElement(buffer, offset, null);
                return (E) e;
            }
        }
    }

    boolean isEmpty() {
        long cIndex = lvConsumerIndex();
        long pIndex = lvProducerIndex();
        pIndex += (pIndex & RESIZE_BIT);
        return cIndex == pIndex;
    }

    /**
     * An estimate of the number of queued elements, >= 0; 0 exactly when isEmpty() would
     * be true at the same two reads.
     */
    int sizeEstimate() {
        long cIndex = lvConsumerIndex();
        long pIndex = lvProducerIndex();
        pIndex += (pIndex & RESIZE_BIT);
        long size = (pIndex - cIndex) >> 1;
        return size <= 0 ? 0 : (int) Math.min(size, Integer.MAX_VALUE);
    }
}
