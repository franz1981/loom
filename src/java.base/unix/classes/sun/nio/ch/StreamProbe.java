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

import java.io.FileDescriptor;
import java.io.IOException;
import jdk.internal.util.OperatingSystem;

/**
 * Whether a stream operation on a file descriptor would wait now, found without waiting,
 * for callers that then begin no Blocker section (Blocker.tryNonBlockingFirst). Every
 * check is false when it cannot tell, and none throws.
 *
 * <p>A result is only as fresh as the poll(2) that gave it, a benign race: another writer
 * of a FIFO may take the room before the write, another reader the bytes before the
 * read. That one operation then blocks its carrier in the syscall until the peer acts,
 * as with no compensation (a peer that waits for a thread homed on that carrier
 * included); no wakeup is lost.
 */
public final class StreamProbe {
    private StreamProbe() { }

    // PIPE_BUF on Linux: a pipe that polls writable has a free page buffer, so it takes a
    // write of at most this many bytes without waiting
    private static final int FIFO_WRITE_NO_WAIT = 4096;

    /** True if fd is a FIFO whose writes fifoTakes can check: Linux only. */
    public static boolean checksFifoWrites(FileDescriptor fd) {
        return OperatingSystem.isLinux() && sun.nio.fs.UnixFdType.isFifo(IOUtil.fdVal(fd));
    }

    /**
     * True if a write of len bytes to the FIFO fd would not wait now: no reader
     * (POLLERR: a write of any length fails at once with a broken pipe), or room for it
     * (POLLOUT, for len up to PIPE_BUF). Any other result, POLLNVAL included, may wait.
     */
    public static boolean fifoTakes(FileDescriptor fd, int len) {
        try {
            int events = Net.poll(fd, Net.POLLOUT, 0);
            return (events & Net.POLLERR) != 0
                    || (len <= FIFO_WRITE_NO_WAIT && (events & Net.POLLOUT) != 0);
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * True if fd's reads can be checked by {@link #readable}: a pipe, FIFO or regular
     * file, where a poll that reports bytes or the end means the read returns now. Not a
     * terminal: in non-canonical mode with VMIN > 1 and VTIME > 0 it polls readable with
     * fewer than VMIN bytes and the read then waits up to VTIME for more.
     */
    public static boolean checksReads(FileDescriptor fd) {
        return sun.nio.fs.UnixFdType.pollInMeansReadable(IOUtil.fdVal(fd));
    }

    /**
     * True if a read from fd would not wait now: bytes (POLLIN, a regular file always),
     * the end of the stream (POLLHUP), or an error that the read reports at once
     * (POLLERR). Any other result may wait: POLLNVAL says only that poll(2) could not
     * poll the fd, nothing about the read.
     */
    public static boolean readable(FileDescriptor fd) {
        try {
            return (Net.poll(fd, Net.POLLIN, 0) & (Net.POLLIN | Net.POLLHUP | Net.POLLERR)) != 0;
        } catch (IOException e) {
            return false;
        }
    }
}
