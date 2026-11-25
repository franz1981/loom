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

package jdk.internal.misc;

/**
 * A carrier thread, other than {@link CarrierThread}, whose scheduler keeps a
 * {@link Blocker} section from blocking it: the MPSC scheduler moves the section's
 * virtual thread to a thread of its offload pool.
 */
public interface BlockingCarrier {
    /**
     * Invoked by the virtual thread mounted on this carrier before it blocks.
     * @return true if {@link #endBlocking()} must be invoked after the operation
     */
    boolean beginBlocking();

    /**
     * Invoked by a virtual thread after a blocking operation whose
     * {@link #beginBlocking()} returned true, on the carrier it is mounted on then,
     * which need not be the one it began on.
     */
    void endBlocking();
}
