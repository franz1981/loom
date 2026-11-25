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
package sun.nio.ch;

import java.io.IOException;

/**
 * An I/O poller owned by one carrier thread (poller Mode 4, CARRIER_LOCAL_POLLER),
 * available when the platform's {@code PollerProvider.supportsLocalPoller()}.
 *
 * <p>A registration maps a file descriptor to the thread waiting on it. It is added
 * by {@link #register} and removed once: by {@link #poll} when the fd is ready, which
 * unparks the thread, or by {@link #deregister}, whichever comes first.
 *
 * <p>Threads: only a virtual thread mounted on the owning carrier, with its
 * continuation pinned, registers. Only the owning carrier polls, and blocks in
 * {@code poll(-1)} when idle. Any thread may deregister or {@link #wakeup}.
 */
public interface LocalPoller {

    /**
     * Registers a file descriptor for the given event; the thread is unparked when
     * it is ready. Called before the thread parks.
     */
    void register(int fdVal, int event, Thread thread) throws IOException;

    /**
     * Removes the given thread's registration of a file descriptor if it is still in
     * place, i.e. the thread was unparked by something other than readiness.
     */
    void deregister(int fdVal, Thread thread);

    /**
     * Polls for ready file descriptors and unparks their threads.
     *
     * @param timeout milliseconds: -1 to block, 0 for non-blocking
     * @return true if the poll returned as many events as one poll can, so more may
     *         be ready
     */
    boolean poll(int timeout) throws IOException;

    /**
     * Whether any registration is in place. On the owning carrier a false result
     * stays false until it registers again.
     */
    boolean hasRegistrations();

    /**
     * Wakes the owning carrier from a blocking {@link #poll}.
     */
    void wakeup() throws IOException;

    /**
     * Releases the poller's resources. Called only on a poller that no carrier uses.
     */
    void close();

    /**
     * A carrier thread that may own a local poller.
     */
    interface Carrier {
        /**
         * Returns this carrier's local poller, or null if it has none. Called by
         * the current carrier only.
         */
        LocalPoller localPoller();
    }

    /**
     * Marker for a built-in virtual thread scheduler whose carriers own local
     * pollers when {@link LocalPollers#enabled()} is true. Poller selects Mode 4
     * only for such a scheduler, and never Mode 3.
     */
    interface Scheduler { }
}
