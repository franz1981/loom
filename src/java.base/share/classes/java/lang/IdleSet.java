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

/**
 * One idle bit per thread of a set, written only by that thread: per carrier with
 * stealing, set when it parks and cleared when it leaves PARKED; per offload pool
 * worker, set when it becomes IDLE and cleared when it takes a task or exits. A bit is
 * a hint; the thread's state is the truth.
 */
final class IdleSet {
    private static final VarHandle IDLE_WORD = MethodHandles.arrayElementVarHandle(long[].class);

    // Thread i's bit is bit i & 63 of word i >>> 6. Words are 128 bytes apart, with
    // padding before the first and after the last, so no two share a cache line.
    private static final int IDLE_STRIDE = 16;
    private final long[] idleWords;
    private final int idleWordCount;

    IdleSet(int size) {
        this.idleWordCount = (size + 63) >>> 6;
        this.idleWords = new long[(idleWordCount + 2) * IDLE_STRIDE];
    }

    int wordCount() {
        return idleWordCount;
    }

    long word(int w) {
        return (long) IDLE_WORD.getVolatile(idleWords, idleIndex(w));
    }

    boolean anyIdle() {
        var words = this.idleWords;
        for (int w = 0; w < idleWordCount; w++) {
            if ((long) IDLE_WORD.getVolatile(words, idleIndex(w)) != 0) {
                return true;
            }
        }
        return false;
    }

    private static int idleIndex(int w) {
        return (w + 1) * IDLE_STRIDE;
    }

    /** The number of bits set, a racy snapshot. */
    int count() {
        int n = 0;
        for (int w = 0; w < idleWordCount; w++) {
            n += Long.bitCount(word(w));
        }
        return n;
    }

    void setIdle(int i) {
        IDLE_WORD.getAndBitwiseOr(idleWords, idleIndex(i >>> 6), 1L << i);
    }

    void clearIdle(int i) {
        IDLE_WORD.getAndBitwiseAnd(idleWords, idleIndex(i >>> 6), ~(1L << i));
    }
}
