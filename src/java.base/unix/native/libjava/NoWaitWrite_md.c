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

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/syscall.h>
#include <sys/uio.h>
#endif

#include "jni.h"
#include "jdk_internal_misc_NoWaitWrite.h"

#define UNSUPPORTED (-1)
#if defined(__linux__) && defined(SYS_pwritev2)

#ifndef RWF_NOWAIT
#define RWF_NOWAIT 0x00000008
#endif

// offset -1 (the current position) in the kernel's lo/hi form, as glibc's pwritev2 passes it
static jint writeNoWait(int fd, char *buf, size_t len) {
    struct iovec iov = { buf, len };
    long lo = -1L;
    long hi = (long) (((uint64_t) (int64_t) -1) >> 32);
    long r = syscall(SYS_pwritev2, fd, &iov, 1, lo, hi, RWF_NOWAIT);
    if (r >= 0) {
        return (jint) r;
    }
    return (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) ? 0 : UNSUPPORTED;
}

/*
 * Writes from the array itself (GetPrimitiveArrayCritical, no copy). Only the syscall
 * runs inside the critical region, and RWF_NOWAIT keeps it from waiting.
 */
JNIEXPORT jint JNICALL
Java_jdk_internal_misc_NoWaitWrite_write0(JNIEnv *env, jclass cls, jint fd,
                                          jbyteArray bytes, jint off, jint len)
{
    jbyte *a;
    jint n;
    if (fd < 0) {
        return 0;
    }
    a = (*env)->GetPrimitiveArrayCritical(env, bytes, NULL);
    if (a == NULL) {
        // out of memory: the caller writes it all in its section
        (*env)->ExceptionClear(env);
        return 0;
    }
    n = writeNoWait(fd, (char *) (a + off), (size_t) len);
    (*env)->ReleasePrimitiveArrayCritical(env, bytes, a, JNI_ABORT);
    return n;
}

JNIEXPORT jint JNICALL
Java_jdk_internal_misc_NoWaitWrite_write1(JNIEnv *env, jclass cls, jint fd, jint b)
{
    char c = (char) b;
    return (fd < 0) ? 0 : writeNoWait(fd, &c, 1);
}

#else

JNIEXPORT jint JNICALL
Java_jdk_internal_misc_NoWaitWrite_write0(JNIEnv *env, jclass cls, jint fd,
                                          jbyteArray bytes, jint off, jint len)
{
    return UNSUPPORTED;
}

JNIEXPORT jint JNICALL
Java_jdk_internal_misc_NoWaitWrite_write1(JNIEnv *env, jclass cls, jint fd, jint b)
{
    return UNSUPPORTED;
}

#endif

JNIEXPORT jboolean JNICALL
Java_jdk_internal_misc_NoWaitWrite_isRegularFile0(JNIEnv *env, jclass cls, jint fd)
{
    struct stat st;
    return (fd >= 0 && fstat(fd, &st) == 0 && S_ISREG(st.st_mode)) ? JNI_TRUE : JNI_FALSE;
}
