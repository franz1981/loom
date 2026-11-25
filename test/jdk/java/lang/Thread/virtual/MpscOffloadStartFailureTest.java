/*
 * Copyright (c) 2026, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
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

/*
 * @test id=mode4
 * @summary MPSC scheduler: an offload pool thread that fails to start (a child JVM whose
 *          cgroup pids.max is lowered) leaves the section on the carrier; a failed start gives
 *          its place back, and repeated failures are not retried at every section
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run main/othervm/timeout=480 MpscOffloadStartFailureTest
 */

/*
 * @test id=mode2
 * @requires vm.continuations & os.family == "linux"
 * @modules java.base/jdk.internal.misc
 * @library /test/lib
 * @run main/othervm/timeout=480 MpscOffloadStartFailureTest -Djdk.pollerMode=2
 */

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jdk.internal.misc.Blocker;
import jdk.test.lib.Utils;
import jdk.test.lib.process.OutputAnalyzer;
import jdk.test.lib.process.ProcessTools;

public class MpscOffloadStartFailureTest {
    private static final double FACTOR = Utils.TIMEOUT_FACTOR;
    private static final long DEADLINE_MS = MpscTestSupport.DEADLINE_MS;

    // sections made while every start fails, in the case "bound"
    private static final int SECTIONS = 400;

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("child")) {
            child(args[1]);
            return;
        }
        if (!scopeWorks()) {
            throw new jtreg.SkippedException("no systemd user scope with a writable pids.max");
        }
        String flag = (args.length > 0) ? args[0] : null;
        OutputAnalyzer fallback = run("fallback", 2, flag);
        OutputAnalyzer bound = run("bound", 17, flag);

        long failed = failedStarts(bound.getOutput());
        System.out.println("bound: failed thread starts=" + failed + " for " + SECTIONS + " sections");
        // 9 expected for 400 sections, plus a few for other JVM threads
        if (failed > 16) {
            throw new RuntimeException("bound: " + failed + " failed thread starts for " + SECTIONS
                    + " sections: a start is tried at nearly every section");
        }
    }

    // ---- the driver ----

    private static boolean scopeWorks() throws Exception {
        try {
            Process p = new ProcessBuilder("systemd-run", "--user", "--scope", "-q", "--",
                    "sh", "-c", "test -w /sys/fs/cgroup$(sed -n 's/^0:://p' /proc/self/cgroup)/pids.max")
                    .inheritIO().start();
            return p.waitFor(30, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (java.io.IOException e) {
            return false;
        }
    }

    private static long failedStarts(String output) {
        return output.lines().filter(l -> l.contains("pthread_create failed")).count();
    }

    private static OutputAnalyzer run(String mode, int maxPoolSize, String flag) throws Exception {
        List<String> opts = new ArrayList<>(List.of(
                "-esa",
                "-Djdk.virtualThreadScheduler.useMpsc=true",
                "-Djdk.virtualThreadScheduler.parallelism=1",
                "-Djdk.virtualThreadScheduler.maxPoolSize=" + maxPoolSize,
                "-Dtest.timeout.factor=" + FACTOR,
                // the JVM's own threads exist before the limit
                "-XX:-UseDynamicNumberOfCompilerThreads",
                "-XX:-UseDynamicNumberOfGCThreads",
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED"));
        if (flag != null) {
            opts.add(flag);
        }
        opts.addAll(List.of("MpscOffloadStartFailureTest", "child", mode));
        ProcessBuilder java = ProcessTools.createTestJavaProcessBuilder(opts);
        List<String> cmd = new ArrayList<>(List.of("systemd-run", "--user", "--scope", "-q", "--"));
        cmd.addAll(java.command());
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.directory(java.directory());
        pb.environment().putAll(java.environment());
        System.out.println("case " + mode + ": " + String.join(" ", cmd));
        Process p = pb.start();
        StringBuilder sb = new StringBuilder();
        Thread reader = Thread.ofPlatform().start(() -> {
            try (var in = new java.io.BufferedReader(new java.io.InputStreamReader(p.getInputStream()))) {
                for (String l; (l = in.readLine()) != null; ) {
                    synchronized (sb) {
                        sb.append(l).append('\n');
                    }
                }
            } catch (java.io.IOException e) { }
        });
        boolean ended = p.waitFor((long) (120 * Math.max(1, FACTOR)), TimeUnit.SECONDS);
        if (!ended) {
            p.destroyForcibly().waitFor();
        }
        reader.join();
        String output;
        synchronized (sb) {
            output = sb.toString();
        }
        System.out.println(output);
        if (!ended) {
            throw new RuntimeException("case " + mode + ": child did not end");
        }
        OutputAnalyzer out = new OutputAnalyzer(output);
        if (failedStarts(output) < 1) {
            throw new RuntimeException("case " + mode + ": no thread start failed: the test proved nothing");
        }
        out.shouldContain("RESULT PASS " + mode);
        if (p.exitValue() != 0) {
            throw new RuntimeException("case " + mode + ": child exit " + p.exitValue());
        }
        return out;
    }

    // ---- the child ----

    private static Path pidsMax;
    private static Path pidsCurrent;

    /** No thread or process can start from here until unlimit(). */
    private static void limit() throws Exception {
        String now = Files.readString(pidsCurrent).strip();
        Files.writeString(pidsMax, now);
        System.out.println("pids.max=" + now);
    }

    private static void unlimit() throws Exception {
        Files.writeString(pidsMax, "max");
        System.out.println("pids.max=max");
    }

    private static void child(String mode) throws Exception {
        String cg = null;
        for (String l : Files.readAllLines(Path.of("/proc/self/cgroup"))) {
            if (l.startsWith("0::")) cg = l.substring(3);
        }
        Path dir = Path.of("/sys/fs/cgroup" + cg);
        pidsMax = dir.resolve("pids.max");
        pidsCurrent = dir.resolve("pids.current");
        // the scheduler, its service thread, and the timer thread of timed parks
        Thread.ofVirtual().start(() -> { }).join();
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) { }
        }).join();
        try {
            switch (mode) {
                case "fallback" -> fallback();
                case "bound" -> bound();
                default -> throw new IllegalArgumentException(mode);
            }
        } finally {
            unlimit();
        }
        System.out.println("RESULT PASS " + mode);
        System.exit(0);
    }

    /**
     * With one pool thread allowed: a section runs on the carrier while starts fail, and one of the
     * next few moves once starts work again.
     */
    private static void fallback() throws Exception {
        List<Holder> holders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            holders.add(new Holder());   // mkfifo forks: before the limit
        }
        limit();
        Holder first = holders.get(0);
        first.start();
        first.awaitEntered();
        check(first.carrier.startsWith(MpscTestSupport.CARRIER),
                "the section did not stay on the carrier: " + first.carrier);
        first.releaseAndJoin();
        unlimit();

        for (int i = 1; i < holders.size(); i++) {
            Holder h = holders.get(i);
            h.start();
            h.awaitEntered();
            boolean moved = h.carrier.startsWith(MpscTestSupport.POOL_THREAD);
            boolean ran = false;
            if (moved) {
                Thread t = Thread.ofVirtual().start(() -> { });
                ran = t.join(Duration.ofMillis(DEADLINE_MS));
            }
            System.out.println("section " + i + " after the limit: on " + h.carrier + ", a new thread ran=" + ran);
            h.releaseAndJoin();
            if (moved) {
                check(ran, "a new thread did not run while a moved section waited");
                return;
            }
        }
        check(false, "no section moved after starts worked again");
    }

    /** While no thread can start, SECTIONS sections in sequence all complete on the carrier. */
    private static void bound() throws Exception {
        limit();
        AtomicReference<String> where = new AtomicReference<>();
        Thread t = Thread.ofVirtual().start(() -> {
            for (int i = 0; i < SECTIONS; i++) {
                boolean a = Blocker.begin();
                try {
                    if (!MpscTestSupport.carrierName().startsWith(MpscTestSupport.CARRIER)) {
                        where.set(MpscTestSupport.carrierName());
                    }
                } finally {
                    Blocker.end(a);
                }
            }
        });
        check(t.join(Duration.ofMillis(DEADLINE_MS)), "the sections did not complete");
        check(where.get() == null, "a section moved while no thread could start: " + where.get());
    }

    private static void check(boolean ok, String what) {
        if (!ok) {
            System.out.println("RESULT FAIL " + what);
            try {
                unlimit();
            } catch (Exception e) { }
            System.exit(1);
        }
    }

    /** A virtual thread in a Blocker section waiting in a FIFO read until released. */
    private static final class Holder {
        final Path fifo;
        final RandomAccessFile writer;
        final RandomAccessFile reader;
        final CountDownLatch entered = new CountDownLatch(1);
        volatile String carrier;
        Thread thread;

        Holder() throws Exception {
            fifo = MpscTestSupport.mkfifo();
            // O_RDWR on both ends: neither open waits for the other
            writer = new RandomAccessFile(fifo.toFile(), "rw");
            reader = new RandomAccessFile(fifo.toFile(), "rw");
        }

        void start() {
            thread = Thread.ofVirtual().start(() -> {
                boolean a = Blocker.begin();
                try {
                    carrier = MpscTestSupport.carrierName();
                    entered.countDown();
                    reader.read();
                } catch (java.io.IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    Blocker.end(a);
                }
            });
        }

        void awaitEntered() throws InterruptedException {
            check(entered.await(DEADLINE_MS, TimeUnit.MILLISECONDS), "holder did not enter");
        }

        /** From this platform thread, which a blocked carrier does not hold. */
        void releaseAndJoin() throws Exception {
            writer.write(1);
            check(thread.join(Duration.ofMillis(DEADLINE_MS)), "holder did not finish");
            writer.close();
            reader.close();
        }
    }
}
