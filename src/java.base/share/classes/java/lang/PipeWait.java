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

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.BooleanSupplier;
import sun.nio.ch.IOUtil;
import sun.nio.ch.Poller;

/**
 * The readiness wait of one subprocess pipe stream (Process.PipeInputStream,
 * Process.PipeOutputStream), and the close it defers. On an MPSC carrier with a local
 * poller (Mode 4), a read or write that would wait parks its virtual thread on a poller
 * ({@link Poller#poll}), as a socket's does, instead of a Blocker section, which would
 * move the thread to the offload pool. Elsewhere the caller takes its other path.
 *
 * <p>The pipe's fd stays in blocking mode. A read after readiness does not wait because
 * this stream is the pipe's only reader (subprocess pipe fds reach no other stream, and
 * the Buffered wrapper's lock serialises its users); a write is a no-wait attempt.
 *
 * <p>exit runs in the callers' finally blocks. A StackOverflowError that skips it leaves
 * the stream BUSY: later operations take the other path, a close stays deferred (the fd
 * stays open), and an interrupt status a wait cleared stays cleared.
 *
 * <p>States of {@code state}:
 * <pre>
 *   OPEN          -> BUSY           an operation enters
 *   BUSY          -> OPEN           it leaves
 *   OPEN          -> CLOSED         close, with no operation in progress: it closes the fd
 *   BUSY          -> CLOSE_PENDING  close during an operation: it returns, the fd stays open
 *   CLOSE_PENDING -> CLOSED         the operation leaves, and closes the fd
 * </pre>
 * <p>Invariants:
 * <ul>
 * <li>The fd is closed only by the thread that moves the state to CLOSED, so never while
 *     an entered operation uses it: no waiter stays registered on a closed fd, nor reads
 *     or writes a reused fd number.
 * <li>A close returns at once and does not end the operation in progress, as close(2)
 *     does not end a blocked read(2) or write(2): the operation completes as a blocked
 *     one would.
 * <li>One operation at a time: another finds BUSY and takes its caller's other path.
 * <li>The wait is not interruptible, as the read or write it replaces: each wait clears
 *     the interrupt status before it parks, and exit sets it again once. Setting it
 *     after each park would leave a park permit and make the next park spin.
 * </ul>
 */
final class PipeWait {
    private static final int OPEN = 0, BUSY = 1, CLOSE_PENDING = 2, CLOSED = 3;
    private static final VarHandle STATE;
    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(PipeWait.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    // a close does not end a wait
    private static final BooleanSupplier ALWAYS = () -> true;

    private volatile int state;
    private final Closeable closer;
    // an interrupt status the operation's waits cleared; only its thread uses it
    private boolean interrupted;

    /** @param closer closes the stream's fd: the stream's own super.close() */
    PipeWait(Closeable closer) {
        this.closer = closer;
    }

    /**
     * True if the current thread may wait for a pipe on a poller: a virtual thread on an
     * MPSC carrier with a local poller (Mode 4). Not in Mode 2, whose pollers are virtual
     * threads that a pinned waiter would keep off its carrier, for good at parallelism 1.
     */
    static boolean canWait() {
        return Thread.currentThread().isVirtual()
                && Thread.currentCarrierThread() instanceof MpscCarrierThread ct
                && ct.localPoller() != null;
    }

    /** True if a close was asked: no operation starts any more. */
    boolean closing() {
        return state >= CLOSE_PENDING;
    }

    /** True if the caller's operation may use the fd and wait: then it must call exit. */
    boolean enter() {
        return STATE.compareAndSet(this, OPEN, BUSY);
    }

    /**
     * Leaves; closes the fd if a close came meanwhile, and sets the interrupt status
     * again if a wait cleared it.
     */
    void exit() {
        boolean wasInterrupted = interrupted;
        interrupted = false;
        if (!STATE.compareAndSet(this, BUSY, OPEN)) {
            state = CLOSED;
            try {
                closer.close();
            } catch (IOException e) {
                // the close was asked and has returned: nobody to report to
            }
        }
        if (wasInterrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called by the stream's close: true if the caller closes the fd now. */
    boolean close() {
        for (;;) {
            int s = state;
            if (s == OPEN) {
                if (STATE.compareAndSet(this, OPEN, CLOSED)) {
                    return true;
                }
            } else if (s == BUSY) {
                if (STATE.compareAndSet(this, BUSY, CLOSE_PENDING)) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }

    /**
     * Parks until fd is ready for event (Net.POLLIN, Net.POLLOUT) or an error, or
     * spuriously, and the caller checks again. Called between enter and exit.
     */
    void await(FileDescriptor fd, int event) throws IOException {
        if (Thread.interrupted()) {
            interrupted = true;
        }
        Poller.poll(IOUtil.fdVal(fd), event, 0, ALWAYS);
    }
}
