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

package jdk.internal.access;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * FileOutputStream's writes without its JFR event, for a stream that splits one write
 * (jdk.internal.misc.NoWaitWrite) and records the one event FileOutputStream would.
 */
public interface JavaIOFileOutputStreamAccess {
    /** True if FileOutputStream's writes record jdk.FileWrite events now. */
    boolean traced();

    /** FileOutputStream.write(b, off, len) of out, without its event. */
    void writeUntraced(FileOutputStream out, byte[] b, int off, int len) throws IOException;

    /** FileOutputStream.write(b) of out, for one byte, without its event. */
    void writeUntraced(FileOutputStream out, int b) throws IOException;
}
