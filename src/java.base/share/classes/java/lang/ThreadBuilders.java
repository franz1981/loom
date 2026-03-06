/*
 * Copyright (c) 2021, 2025, Oracle and/or its affiliates. All rights reserved.
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

import java.lang.Thread.Builder.OfPlatform;
import java.lang.Thread.Builder.OfVirtual;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.misc.Unsafe;
import jdk.internal.invoke.MhUtil;
import jdk.internal.vm.ContinuationSupport;

/**
 * Defines static methods to create platform and virtual thread builders.
 */
class ThreadBuilders {
    private ThreadBuilders() { }

    /**
     * Resolves the worker index from the given thread. If the thread is a
     * virtual thread with an affinity index, that index is copied.
     * Returns -1 otherwise.
     */
    static int resolveWorkerIndex(Thread t) {
        if (t instanceof VirtualThread vt) {
            return vt.affinityWorkerIndex;
        }
        return -1;
    }

    /**
     * Base class for Thread.Builder implementations.
     */
    private static class BaseThreadBuilder {
        private String name;
        private long counter;
        private int characteristics;
        private UncaughtExceptionHandler uhe;

        String name() {
            return name;
        }

        long counter() {
            return counter;
        }

        int characteristics() {
            return characteristics;
        }

        UncaughtExceptionHandler uncaughtExceptionHandler() {
            return uhe;
        }

        String nextThreadName() {
            if (name != null && counter >= 0) {
                return name + (counter++);
            } else {
                return name;
            }
        }

        void setName(String name) {
            this.name = Objects.requireNonNull(name);
            this.counter = -1;
        }

        void setName(String prefix, long start) {
            Objects.requireNonNull(prefix);
            if (start < 0)
                throw new IllegalArgumentException("'start' is negative");
            this.name = prefix;
            this.counter = start;
        }

        void setInheritInheritableThreadLocals(boolean inherit) {
            if (inherit) {
                characteristics &= ~Thread.NO_INHERIT_THREAD_LOCALS;
            } else {
                characteristics |= Thread.NO_INHERIT_THREAD_LOCALS;
            }
        }

        void setUncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
            this.uhe = Objects.requireNonNull(ueh);
        }
    }

    /**
     * ThreadBuilder.OfPlatform implementation.
     */
    static final class PlatformThreadBuilder
            extends BaseThreadBuilder implements OfPlatform {
        private ThreadGroup group;
        private boolean daemon;
        private boolean daemonChanged;
        private int priority;
        private long stackSize;

        PlatformThreadBuilder() {
        }

        @Override
        String nextThreadName() {
            String name = super.nextThreadName();
            return (name != null) ? name : Thread.genThreadName();
        }

        @Override
        public OfPlatform name(String name) {
            setName(name);
            return this;
        }

        @Override
        public OfPlatform name(String prefix, long start) {
            setName(prefix, start);
            return this;
        }

        @Override
        public OfPlatform inheritInheritableThreadLocals(boolean inherit) {
            setInheritInheritableThreadLocals(inherit);
            return this;
        }

        @Override
        public OfPlatform uncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
            setUncaughtExceptionHandler(ueh);
            return this;
        }

        @Override
        public OfPlatform group(ThreadGroup group) {
            this.group = Objects.requireNonNull(group);
            return this;
        }

        @Override
        public OfPlatform daemon(boolean on) {
            daemon = on;
            daemonChanged = true;
            return this;
        }

        @Override
        public OfPlatform priority(int priority) {
            if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY)
                throw new IllegalArgumentException();
            this.priority = priority;
            return this;
        }

        @Override
        public OfPlatform stackSize(long stackSize) {
            if (stackSize < 0L)
                throw new IllegalArgumentException();
            this.stackSize = stackSize;
            return this;
        }

        @Override
        public Thread unstarted(Runnable task) {
            Objects.requireNonNull(task);
            String name = nextThreadName();
            var thread = new Thread(group, name, characteristics(), task, stackSize);
            if (daemonChanged)
                thread.daemon(daemon);
            if (priority != 0)
                thread.priority(priority);
            UncaughtExceptionHandler uhe = uncaughtExceptionHandler();
            if (uhe != null)
                thread.uncaughtExceptionHandler(uhe);
            return thread;
        }

        @Override
        public Thread start(Runnable task) {
            Thread thread = unstarted(task);
            thread.start();
            return thread;
        }

        @Override
        public ThreadFactory factory() {
            return new PlatformThreadFactory(group, name(), counter(), characteristics(),
                    daemonChanged, daemon, priority, stackSize, uncaughtExceptionHandler());
        }
    }

    /**
     * ThreadBuilder.OfVirtual implementation.
     */
    static final class VirtualThreadBuilder
            extends BaseThreadBuilder implements OfVirtual {
        private Thread.VirtualThreadScheduler scheduler;
        private boolean useRoundRobinAffinity;
        private boolean useCallerAffinity;
        private boolean useLocalStart;
        private int inheritedAffinityWorkerIndex = -1;
        private AtomicInteger roundRobinCounter;

        VirtualThreadBuilder() {
        }

        @Override
        public OfVirtual name(String name) {
            setName(name);
            return this;
        }

        @Override
        public OfVirtual name(String prefix, long start) {
            setName(prefix, start);
            return this;
        }

        @Override
        public OfVirtual inheritInheritableThreadLocals(boolean inherit) {
            setInheritInheritableThreadLocals(inherit);
            return this;
        }

        @Override
        public OfVirtual uncaughtExceptionHandler(UncaughtExceptionHandler ueh) {
            setUncaughtExceptionHandler(ueh);
            return this;
        }

        @Override
        public OfVirtual roundRobinAffinity() {
            this.useRoundRobinAffinity = true;
            this.useCallerAffinity = false;
            this.inheritedAffinityWorkerIndex = -1;
            this.roundRobinCounter = new AtomicInteger();
            return this;
        }

        @Override
        public OfVirtual inheritAffinity(Thread t) {
            this.useRoundRobinAffinity = false;
            this.useCallerAffinity = false;
            this.inheritedAffinityWorkerIndex = resolveWorkerIndex(t);
            return this;
        }

        @Override
        public OfVirtual inheritAffinity() {
            this.useRoundRobinAffinity = false;
            this.useCallerAffinity = true;
            this.inheritedAffinityWorkerIndex = -1;
            return this;
        }

        @Override
        public OfVirtual inheritLocality() {
            this.useLocalStart = true;
            return this;
        }

        Thread unstarted(Runnable task, Thread preferredCarrier) {
            Objects.requireNonNull(task);
            var thread = newVirtualThread(scheduler,
                                          preferredCarrier,
                                          nextThreadName(),
                                          characteristics(),
                                          task,
                                          resolveAffinityIndex(),
                                          useLocalStart);
            UncaughtExceptionHandler uhe = uncaughtExceptionHandler();
            if (uhe != null)
                thread.uncaughtExceptionHandler(uhe);
            return thread;
        }

        private int resolveAffinityIndex() {
            if (useRoundRobinAffinity) {
                ForkJoinPool pool = resolvePool();
                if (pool != null) {
                    int parallelism = pool.getParallelism();
                    if (parallelism > 0) {
                        return Math.floorMod(roundRobinCounter.getAndIncrement(), parallelism);
                    }
                }
            }
            if (useCallerAffinity) {
                return resolveWorkerIndex(Thread.currentThread());
            }
            return inheritedAffinityWorkerIndex;
        }

        private ForkJoinPool resolvePool() {
            if (scheduler == null) {
                var defaultScheduler = VirtualThread.defaultScheduler();
                if (defaultScheduler instanceof ForkJoinPool pool) {
                    return pool;
                }
            }
            return null;
        }

        @Override
        public Thread unstarted(Runnable task) {
            return unstarted(task, null);
        }

        @Override
        public Thread start(Runnable task) {
            Thread thread = unstarted(task);
            thread.start();
            return thread;
        }

        @Override
        public ThreadFactory factory() {
            return new VirtualThreadFactory(scheduler, name(), counter(), characteristics(),
                    uncaughtExceptionHandler(), useRoundRobinAffinity,
                    useCallerAffinity, useLocalStart, inheritedAffinityWorkerIndex);
        }
    }

    /**
     * Base ThreadFactory implementation.
     */
    private abstract static class BaseThreadFactory implements ThreadFactory {
        private static final VarHandle COUNT = MhUtil.findVarHandle(
                MethodHandles.lookup(), "count", long.class);

        private final String name;
        private final int characteristics;
        private final UncaughtExceptionHandler uhe;

        private final boolean hasCounter;
        private volatile long count;

        BaseThreadFactory(String name,
                          long start,
                          int characteristics,
                          UncaughtExceptionHandler uhe)  {
            this.name = name;
            if (name != null && start >= 0) {
                this.hasCounter = true;
                this.count = start;
            } else {
                this.hasCounter = false;
            }
            this.characteristics = characteristics;
            this.uhe = uhe;
        }

        int characteristics() {
            return characteristics;
        }

        UncaughtExceptionHandler uncaughtExceptionHandler() {
            return uhe;
        }

        String nextThreadName() {
            if (hasCounter) {
                return name + (long) COUNT.getAndAdd(this, 1L);
            } else {
                return name;
            }
        }
    }

    /**
     * ThreadFactory for platform threads.
     */
    private static class PlatformThreadFactory extends BaseThreadFactory {
        private final ThreadGroup group;
        private final boolean daemonChanged;
        private final boolean daemon;
        private final int priority;
        private final long stackSize;

        PlatformThreadFactory(ThreadGroup group,
                              String name,
                              long start,
                              int characteristics,
                              boolean daemonChanged,
                              boolean daemon,
                              int priority,
                              long stackSize,
                              UncaughtExceptionHandler uhe) {
            super(name, start, characteristics, uhe);
            this.group = group;
            this.daemonChanged = daemonChanged;
            this.daemon = daemon;
            this.priority = priority;
            this.stackSize = stackSize;
        }

        @Override
        String nextThreadName() {
            String name = super.nextThreadName();
            return (name != null) ? name : Thread.genThreadName();
        }

        @Override
        public Thread newThread(Runnable task) {
            Objects.requireNonNull(task);
            String name = nextThreadName();
            Thread thread = new Thread(group, name, characteristics(), task, stackSize);
            if (daemonChanged)
                thread.daemon(daemon);
            if (priority != 0)
                thread.priority(priority);
            UncaughtExceptionHandler uhe = uncaughtExceptionHandler();
            if (uhe != null)
                thread.uncaughtExceptionHandler(uhe);
            return thread;
        }
    }

    /**
     * ThreadFactory for virtual threads.
     */
    private static class VirtualThreadFactory extends BaseThreadFactory {
        private final Thread.VirtualThreadScheduler scheduler;
        private final boolean useRoundRobinAffinity;
        private final boolean useCallerAffinity;
        private final boolean useLocalStart;
        private final int inheritedAffinityWorkerIndex;
        private final AtomicInteger roundRobinCounter;

        VirtualThreadFactory(Thread.VirtualThreadScheduler scheduler,
                             String name,
                             long start,
                             int characteristics,
                             UncaughtExceptionHandler uhe,
                             boolean useRoundRobinAffinity,
                             boolean useCallerAffinity,
                             boolean useLocalStart,
                             int inheritedAffinityWorkerIndex) {
            super(name, start, characteristics, uhe);
            this.scheduler = scheduler;
            this.useRoundRobinAffinity = useRoundRobinAffinity;
            this.useCallerAffinity = useCallerAffinity;
            this.useLocalStart = useLocalStart;
            this.inheritedAffinityWorkerIndex = inheritedAffinityWorkerIndex;
            this.roundRobinCounter = useRoundRobinAffinity ? new AtomicInteger() : null;
        }

        @Override
        public Thread newThread(Runnable task) {
            Objects.requireNonNull(task);
            String name = nextThreadName();
            int affinityIndex = resolveAffinityIndex();
            Thread thread = newVirtualThread(scheduler, null, name, characteristics(),
                    task, affinityIndex, useLocalStart);
            UncaughtExceptionHandler uhe = uncaughtExceptionHandler();
            if (uhe != null)
                thread.uncaughtExceptionHandler(uhe);
            return thread;
        }

        private int resolveAffinityIndex() {
            if (useRoundRobinAffinity) {
                ForkJoinPool pool = resolvePool();
                if (pool != null) {
                    int parallelism = pool.getParallelism();
                    if (parallelism > 0) {
                        return Math.floorMod(roundRobinCounter.getAndIncrement(), parallelism);
                    }
                }
            }
            if (useCallerAffinity) {
                return resolveWorkerIndex(Thread.currentThread());
            }
            return inheritedAffinityWorkerIndex;
        }

        /**
         * Resolves the ForkJoinPool from the scheduler. Returns the builtin
         * scheduler pool if no scheduler is set (or is the builtin one).
         * Returns null otherwise.
         */
        private ForkJoinPool resolvePool() {
            if (scheduler == null) {
                // null means builtin scheduler, which is a ForkJoinPool
                var defaultScheduler = VirtualThread.defaultScheduler();
                if (defaultScheduler instanceof ForkJoinPool pool) {
                    return pool;
                }
            }
            return null;
        }
    }

    /**
     * Creates a new virtual thread to run the given task.
     */
    private static Thread newVirtualThread(Thread.VirtualThreadScheduler scheduler,
                                           Thread preferredCarrier,
                                           String name,
                                           int characteristics,
                                           Runnable task) {
        return newVirtualThread(scheduler, preferredCarrier, name, characteristics, task, -1);
    }

    /**
     * Creates a new virtual thread to run the given task with an affinity hint.
     */
    private static Thread newVirtualThread(Thread.VirtualThreadScheduler scheduler,
                                           Thread preferredCarrier,
                                           String name,
                                           int characteristics,
                                           Runnable task,
                                           int affinityWorkerIndex) {
        return newVirtualThread(scheduler, preferredCarrier, name, characteristics,
                task, affinityWorkerIndex, false);
    }

    /**
     * Creates a new virtual thread to run the given task with an affinity hint
     * and optional local-start behavior.
     */
    private static Thread newVirtualThread(Thread.VirtualThreadScheduler scheduler,
                                           Thread preferredCarrier,
                                           String name,
                                           int characteristics,
                                           Runnable task,
                                           int affinityWorkerIndex,
                                           boolean localStart) {
        if (ContinuationSupport.isSupported()) {
            return new VirtualThread(scheduler, preferredCarrier, name, characteristics,
                    task, affinityWorkerIndex, localStart);
        } else {
            if (scheduler != null)
                throw new UnsupportedOperationException();
            return new BoundVirtualThread(name, characteristics, task);
        }
    }

    static Thread newVirtualThread(String name, int characteristics, Runnable task) {
       return newVirtualThread(null, null, name, characteristics, task);
    }

    /**
     * A "virtual thread" that is backed by a platform thread. This implementation
     * is intended for platforms that don't have the underlying VM support for
     * continuations. It can also be used for testing.
     */
    static final class BoundVirtualThread extends BaseVirtualThread {
        private static final Unsafe U = Unsafe.getUnsafe();
        private final Runnable task;
        private boolean runInvoked;

        BoundVirtualThread(String name, int characteristics, Runnable task) {
            super(name, characteristics, true);
            this.task = task;
        }

        @Override
        public void run() {
            // run is specified to do nothing when Thread is a virtual thread
            if (Thread.currentThread() == this && !runInvoked) {
                runInvoked = true;
                Object bindings = Thread.scopedValueBindings();
                runWith(bindings, task);
            }
        }

        @Override
        void park() {
            U.park(false, 0L);
        }

        @Override
        void parkNanos(long nanos) {
            U.park(false, nanos);
        }

        @Override
        void unpark() {
            U.unpark(this);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("VirtualThread[#");
            sb.append(threadId());
            String name = getName();
            if (!name.isEmpty()) {
                sb.append(",");
                sb.append(name);
            }
            sb.append("]/");
            String stateAsString = threadState().toString();
            sb.append(stateAsString.toLowerCase(Locale.ROOT));
            return sb.toString();
        }
    }
}
