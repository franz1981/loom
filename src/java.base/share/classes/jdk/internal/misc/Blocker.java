/*
 * Copyright (c) 2019, 2024, Oracle and/or its affiliates. All rights reserved.
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
import jdk.internal.access.JavaIOFileDescriptorAccess;
import jdk.internal.access.JavaLangAccess;
import jdk.internal.access.SharedSecrets;

/**
 * Defines static methods to mark the beginning and end of a possibly blocking
 * operation. The methods are intended to be used with try-finally as follows:
 * {@snippet lang=java :
 *     boolean attempted = Blocker.begin();
 *     try {
 *         // blocking operation
 *     } finally {
 *         Blocker.end(attempted);
 *     }
 * }
 * If invoked from a virtual thread and the underlying carrier thread is a
 * CarrierThread then the code in the block runs as if it were in run in
 * ForkJoinPool.ManagedBlocker. This means the pool can be expanded to support
 * additional parallelism during the blocking operation. If the carrier is a
 * {@link BlockingCarrier}, its scheduler may run the block on another platform thread.
 *
 * <p>So between begin and end the virtual thread may run on another platform thread
 * than the one it began on, and end may be called on a third. A site must not hold,
 * across begin, state that belongs to the carrier or to its OS thread: carrier-thread
 * locals, a native thread id or signal target, OS-thread-affine resources. State of
 * the virtual thread (its locals, scoped values, interrupt status, monitors and locks)
 * and of the process (fds, POSIX record locks) is unaffected.
 */
public class Blocker {
    private static final JavaLangAccess JLA;
    static {
        JLA = SharedSecrets.getJavaLangAccess();
        if (JLA == null) {
            throw new InternalError("JavaLangAccess not setup");
        }
    }
    private static final JavaIOFileDescriptorAccess FD_ACCESS =
            SharedSecrets.getJavaIOFileDescriptorAccess();

    private Blocker() { }

    private static Thread currentCarrierThread() {
        return JLA.currentCarrierThread();
    }

    /**
     * Marks the beginning of a blocking operation.
     * @return true if the carrier's scheduler acts, and end must be told
     */
    public static boolean begin() {
        if (VM.isBooted() && Thread.currentThread().isVirtual()) {
            Thread carrier = currentCarrierThread();
            if (carrier instanceof CarrierThread ct) {
                return ct.beginBlocking();
            }
            if (carrier instanceof BlockingCarrier bc) {
                return bc.beginBlocking();
            }
        }
        return false;
    }

    /**
     * Whether a site should first try its operation without blocking, and begin a
     * section only for what that cannot finish. True on a virtual thread mounted on a
     * {@link BlockingCarrier}, where a section costs a move to another platform thread
     * and back. Elsewhere sites keep their sections as they are.
     */
    public static boolean tryNonBlockingFirst() {
        return VM.isBooted() && Thread.currentThread().isVirtual()
                && currentCarrierThread() instanceof BlockingCarrier;
    }

    /**
     * Marks the beginning of a possibly blocking operation.
     * @param blocking true if the operation may block, otherwise false
     * @return true if the carrier's scheduler acts, and end must be told
     */
    public static boolean begin(boolean blocking) {
        return (blocking) ? begin() : false;
    }

    /**
     * Marks the beginning of a disk operation on the file of fd that may block: a direct
     * or synchronous transfer, or a flush. A CarrierThread compensates for it as for
     * {@link #begin()}. On a BlockingCarrier it runs on the carrier if the file is a
     * regular file on a local filesystem, which waits on no other party (a slow disk
     * holds the carrier for the call); any other file is a section as {@link #begin()}
     * makes it. The file's kind is read from fd once, inside the section of its first
     * such operation on a BlockingCarrier, since the read may wait on a remote server.
     * @param fd the file's descriptor, or null if not known (then not local)
     * @param blocking true if the operation may block, otherwise false
     * @return true if the carrier's scheduler acts, and end must be told
     */
    public static boolean beginDiskIO(FileDescriptor fd, boolean blocking) {
        if (blocking && VM.isBooted() && Thread.currentThread().isVirtual()) {
            Thread carrier = currentCarrierThread();
            if (carrier instanceof CarrierThread ct) {
                return ct.beginBlocking();
            }
            if (carrier instanceof BlockingCarrier bc) {
                int kind = (fd == null) ? JavaIOFileDescriptorAccess.FS_OTHER : FD_ACCESS.fsKind(fd);
                if (kind == JavaIOFileDescriptorAccess.FS_LOCAL) {
                    return false;
                }
                boolean attempted = bc.beginBlocking();
                if (kind == JavaIOFileDescriptorAccess.FS_UNKNOWN) {
                    FD_ACCESS.fsClassify(fd);
                }
                return attempted;
            }
        }
        return false;
    }

    /**
     * Marks the end of an operation that may have blocked.
     * @param attempted the value begin returned
     */
    public static void end(boolean attempted) {
        if (attempted) {
            Thread carrier = currentCarrierThread();
            if (carrier instanceof CarrierThread ct) {
                ct.endBlocking();
            } else if (carrier instanceof BlockingCarrier bc) {
                bc.endBlocking();
            }
        }
    }
}
