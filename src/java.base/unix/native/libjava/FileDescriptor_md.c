/*
 * Copyright (c) 1997, 2023, Oracle and/or its affiliates. All rights reserved.
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

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

#if defined(__linux__)
#include <sys/vfs.h>
#elif defined(_ALLBSD_SOURCE)
#include <sys/param.h>
#include <sys/mount.h>
#endif

#include "jni.h"
#include "jni_util.h"
#include "jvm.h"

#include "io_util_md.h"
#include "java_io_FileDescriptor.h"

/*******************************************************************/
/*  BEGIN JNI ********* BEGIN JNI *********** BEGIN JNI ************/
/*******************************************************************/

/* field id for jint 'fd' in java.io.FileDescriptor */
jfieldID IO_fd_fdID;

/* field id for jboolean 'append' in java.io.FileDescriptor */
jfieldID IO_append_fdID;

/**************************************************************
 * static methods to store field ID's in initializers
 */

JNIEXPORT void JNICALL
Java_java_io_FileDescriptor_initIDs(JNIEnv *env, jclass fdClass) {
    CHECK_NULL(IO_fd_fdID = (*env)->GetFieldID(env, fdClass, "fd", "I"));
    CHECK_NULL(IO_append_fdID = (*env)->GetFieldID(env, fdClass, "append", "Z"));
}

/**************************************************************
 * File Descriptor
 */

JNIEXPORT void JNICALL
Java_java_io_FileDescriptor_sync0(JNIEnv *env, jobject this) {
    FD fd = THIS_FD(this);
    if (IO_Sync(fd) == -1) {
        JNU_ThrowByName(env, "java/io/SyncFailedException", "sync failed");
    }
}
JNIEXPORT jlong JNICALL
Java_java_io_FileDescriptor_getHandle(JNIEnv *env, jclass fdClass, jint fd) {
    return -1;
}

JNIEXPORT jboolean JNICALL
Java_java_io_FileDescriptor_getAppend(JNIEnv *env, jclass fdClass, jint fd) {
    int flags = fcntl(fd, F_GETFL);
    return ((flags & O_APPEND) == 0) ? JNI_FALSE : JNI_TRUE;
}

/*
 * For FileDescriptor.fsClassify: java_io_FileDescriptor_FS_LOCAL if fd is a regular
 * file on a local filesystem, else java_io_FileDescriptor_FS_OTHER (also when fstat or
 * fstatfs fails). On Linux the filesystem type must be on an allow-list: a type missing
 * from it is treated as other, which costs a hand-off per synchronous operation, never
 * a carrier held by a remote wait. On macOS and the BSDs it is MNT_LOCAL. Elsewhere
 * every file is other.
 */
#if defined(__linux__)
// Inlined from <linux/magic.h> for portability.
#define MAGIC_EXT4       0xEF53       /* ext2, ext3, ext4 */
#define MAGIC_XFS        0x58465342
#define MAGIC_BTRFS      0x9123683E
#define MAGIC_F2FS       0xF2F52010
#define MAGIC_BCACHEFS   0xca451a4e
#define MAGIC_TMPFS      0x01021994
#define MAGIC_RAMFS      0x858458f6
#define MAGIC_HUGETLBFS  0x958458f6
#define MAGIC_OVERLAYFS  0x794c7630
#define MAGIC_MSDOS      0x4d44       /* vfat */
#define MAGIC_EXFAT      0x2011BAB0
// From OpenZFS include/sys/fs/zfs.h (ZFS_SUPER_MAGIC).
#define MAGIC_ZFS        0x2fc12fc1
#endif

#if defined(__linux__) || defined(_ALLBSD_SOURCE)
static int isLocalFs(const struct statfs *st) {
#if defined(__linux__)
    switch ((unsigned int) st->f_type) {
    case MAGIC_EXT4:
    case MAGIC_XFS:
    case MAGIC_BTRFS:
    case MAGIC_F2FS:
    case MAGIC_BCACHEFS:
    case MAGIC_TMPFS:
    case MAGIC_RAMFS:
    case MAGIC_HUGETLBFS:
    case MAGIC_OVERLAYFS:
    case MAGIC_MSDOS:
    case MAGIC_EXFAT:
    case MAGIC_ZFS:
        return 1;
    default:
        return 0;
    }
#else
    return (st->f_flags & MNT_LOCAL) != 0;
#endif
}
#endif

JNIEXPORT jint JNICALL
Java_java_io_FileDescriptor_fsKind0(JNIEnv *env, jclass fdClass, jint fd) {
#if defined(__linux__) || defined(_ALLBSD_SOURCE)
    struct statfs st;
    struct stat sb;
    int r;
    // fstatfs first: on a remote filesystem fstat is itself a request to the server
    RESTARTABLE(fstatfs(fd, &st), r);
    if (r != 0 || !isLocalFs(&st)) {
        return java_io_FileDescriptor_FS_OTHER;
    }
    RESTARTABLE(fstat(fd, &sb), r);
    if (r != 0 || !S_ISREG(sb.st_mode)) {
        return java_io_FileDescriptor_FS_OTHER;
    }
    return java_io_FileDescriptor_FS_LOCAL;
#else
    return java_io_FileDescriptor_FS_OTHER;
#endif
}

// instance method close0 for FileDescriptor
JNIEXPORT void JNICALL
Java_java_io_FileDescriptor_close0(JNIEnv *env, jobject this) {
    fileDescriptorClose(env, this);
}

JNIEXPORT void JNICALL
Java_java_io_FileCleanable_cleanupClose0(JNIEnv *env, jclass fdClass, jint fd, jlong unused) {
    if (fd != -1) {
        if (close(fd) == -1) {
            JNU_ThrowIOExceptionWithLastError(env, "close failed");
        }
    }
}
