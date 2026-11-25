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

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import jdk.internal.access.JavaIOFileDescriptorAccess;
import jdk.internal.access.JavaIOFileOutputStreamAccess;
import jdk.internal.access.SharedSecrets;
import jdk.internal.event.FileWriteEvent;
import sun.nio.ch.StreamProbe;

/**
 * The write of an output stream that normally does not wait, and waits only while the
 * other side stalls: System.out, System.err and a subprocess's input pipe. On a
 * {@link BlockingCarrier} the write is first tried without waiting (pwritev2 with
 * RWF_NOWAIT, Linux), and only the rest is left for the caller to write in a Blocker
 * section. Elsewhere the caller writes everything in its section.
 *
 * <p>States of {@code mode}, decided per stream at its first write on a BlockingCarrier,
 * in a Blocker section (the fd's checks may wait on a remote server):
 * <pre>
 *   UNKNOWN -> REGULAR   a regular file on a local filesystem (System.out and System.err
 *                        only)
 *   UNKNOWN -> NEVER     any other regular file: the whole write is in a section
 *   UNKNOWN -> ATTEMPT   anything else; a subprocess pipe starts in ATTEMPT
 *   ATTEMPT -> FIFO      the kernel refused an attempt, on a FIFO StreamProbe checks
 *   ATTEMPT -> NEVER     the kernel refused an attempt, on any other fd
 * </pre>
 * REGULAR is written plainly, as FileOutputStream writes a regular file, so a short
 * attempt never splits a line that another writer of the file could land inside. FIFO
 * is written plainly when {@link StreamProbe#fifoTakes} says the write would not wait,
 * else in a section; NEVER always in a section. The fd is never checked again: after a
 * native dup2 onto it the stale mode is kept.
 *
 * <p>Invariants: callers hold the stream's lock across a whole write, attempt and
 * remainder, so the bytes of one write stay together. The attempt never throws; the
 * write that follows reports any error. While FileOutputStream records jdk.FileWrite,
 * callers write through {@link #writeRecorded}, which records the one event
 * FileOutputStream would record for the whole write.
 */
public final class NoWaitWrite {
    private static final JavaIOFileDescriptorAccess FD_ACCESS =
            SharedSecrets.getJavaIOFileDescriptorAccess();
    private static final JavaIOFileOutputStreamAccess FOS_ACCESS =
            SharedSecrets.getJavaIOFileOutputStreamAccess();

    /** Returned by write: the caller writes everything itself, with no section. */
    public static final int PLAIN = -1;

    // returned by the natives: the kernel refused the attempt
    private static final int UNSUPPORTED = -1;

    private static final int UNKNOWN = 0, REGULAR = 1, ATTEMPT = 2, NEVER = 3, FIFO = 4;

    private final FileDescriptor fd;
    // racy: a late first write may store ATTEMPT over FIFO or NEVER, costing one more
    // refused attempt
    private int mode;

    /**
     * @param fd the stream's file descriptor
     * @param mayBeRegularFile whether the fd can be a regular file (then it is checked
     *     once): System.out and System.err, not a subprocess pipe
     */
    public NoWaitWrite(FileDescriptor fd, boolean mayBeRegularFile) {
        this.fd = fd;
        this.mode = mayBeRegularFile ? UNKNOWN : ATTEMPT;
    }

    /**
     * Writes what can be written of b[off, off + len) without waiting.
     * @return PLAIN if the caller writes it all with no section (a regular file, a
     *     FIFO with room for it now, or nothing to write on a BlockingCarrier); else
     *     the number of bytes written (0 to len), the rest being the caller's to write
     *     in a section
     */
    public int write(byte[] b, int off, int len) {
        if (!Blocker.tryNonBlockingFirst()) {
            return 0;
        }
        if (len == 0) {
            // nothing to wait for; the caller's write checks the range
            return PLAIN;
        }
        int mode = mode();
        if (mode == REGULAR) {
            return PLAIN;
        }
        if (mode == NEVER || b == null || off < 0 || len <= 0 || len > b.length - off) {
            // NEVER, or a bad range that the caller's write reports
            return 0;
        }
        if (mode == ATTEMPT) {
            int n = write0(FD_ACCESS.get(fd), b, off, len);
            if (n != UNSUPPORTED) {
                return n;
            }
            if (unsupported() == NEVER) {
                return 0;
            }
        }
        return StreamProbe.fifoTakes(fd, len) ? PLAIN : 0;
    }

    /** As {@link #write(byte[], int, int)}, for one byte: PLAIN, 0 or 1. */
    public int write(int b) {
        if (!Blocker.tryNonBlockingFirst()) {
            return 0;
        }
        int mode = mode();
        if (mode == REGULAR) {
            return PLAIN;
        }
        if (mode == NEVER) {
            return 0;
        }
        if (mode == ATTEMPT) {
            int n = write1(FD_ACCESS.get(fd), b);
            if (n != UNSUPPORTED) {
                return n;
            }
            if (unsupported() == NEVER) {
                return 0;
            }
        }
        return StreamProbe.fifoTakes(fd, 1) ? PLAIN : 0;
    }

    /**
     * True if this stream's writes are tried without waiting (ATTEMPT), so a caller that
     * waits for room (POLLOUT) can try again. False while the mode is UNKNOWN.
     */
    public boolean attempts() {
        return mode == ATTEMPT;
    }

    /** The attempt is not supported here: FIFO if its writes can be checked, else NEVER. */
    private int unsupported() {
        int m = StreamProbe.checksFifoWrites(fd) ? FIFO : NEVER;
        mode = m;
        return m;
    }

    /**
     * True if the caller's write goes through writeRecorded: on a BlockingCarrier, while
     * FileOutputStream records jdk.FileWrite events.
     */
    public boolean recorded() {
        return FOS_ACCESS.traced() && Blocker.tryNonBlockingFirst();
    }

    /**
     * The whole write of b[off, off + len) to out, the stream of this fd: the attempt,
     * then the remainder in a section, both without FileOutputStream's event, and the one
     * event FileOutputStream would record (the bytes of the whole write, or 0 if it
     * threw; no path, as out was made from an fd).
     */
    public void writeRecorded(FileOutputStream out, byte[] b, int off, int len) throws IOException {
        long written = 0;
        long start = FileWriteEvent.timestamp();
        try {
            int n = write(b, off, len);
            if (n == PLAIN) {
                FOS_ACCESS.writeUntraced(out, b, off, len);
            } else if (n != len || len <= 0) {
                boolean attempted = Blocker.begin();
                try {
                    FOS_ACCESS.writeUntraced(out, b, off + n, len - n);
                } finally {
                    Blocker.end(attempted);
                }
            }
            written = len;
        } finally {
            FileWriteEvent.offer(start, null, written);
        }
    }

    /** As {@link #writeRecorded(FileOutputStream, byte[], int, int)}, for one byte. */
    public void writeRecorded(FileOutputStream out, int b) throws IOException {
        long written = 0;
        long start = FileWriteEvent.timestamp();
        try {
            int n = write(b);
            if (n == PLAIN) {
                FOS_ACCESS.writeUntraced(out, b);
            } else if (n != 1) {
                boolean attempted = Blocker.begin();
                try {
                    FOS_ACCESS.writeUntraced(out, b);
                } finally {
                    Blocker.end(attempted);
                }
            }
            written = 1;
        } finally {
            FileWriteEvent.offer(start, null, written);
        }
    }

    // decided at the first write where Blocker.tryNonBlockingFirst() holds
    private int mode() {
        int m = mode;
        if (m == UNKNOWN) {
            boolean attempted = Blocker.begin();
            try {
                if (FD_ACCESS.fsClassify(fd) == JavaIOFileDescriptorAccess.FS_LOCAL) {
                    m = REGULAR;
                } else {
                    m = isRegularFile0(FD_ACCESS.get(fd)) ? NEVER : ATTEMPT;
                }
            } finally {
                Blocker.end(attempted);
            }
            mode = m;
        }
        return m;
    }

    // bytes written (> 0), 0 if it would wait or was interrupted, or nothing to do (fd -1),
    // UNSUPPORTED for anything else
    private static native int write0(int fd, byte[] b, int off, int len);

    private static native int write1(int fd, int b);

    // false if it cannot tell
    private static native boolean isRegularFile0(int fd);
}
