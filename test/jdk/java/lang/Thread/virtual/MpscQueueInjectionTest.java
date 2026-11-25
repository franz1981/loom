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
 * @test
 * @summary A StackOverflowError at the entry of any call a run queue offer makes after its
 *          claim leaves the queue usable and in order
 * @library /test/lib
 * @run main/othervm MpscQueueInjectionTest
 */

import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.classfile.CodeElement;
import java.lang.classfile.CodeTransform;
import java.lang.classfile.MethodModel;
import java.lang.classfile.MethodTransform;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import jdk.test.lib.Utils;
import jdk.test.lib.process.ProcessTools;

public class MpscQueueInjectionTest {
    private static final long CHILD_TIMEOUT_MS = Utils.adjustTimeout(60_000);
    private static final Set<String> HOOKED =
            Set.of("soProducerLimit", "soProducerIndex", "soRefElement", "lvConsumerIndex");

    /**
     * The offer kinds, by queue capacity: the second offer to a queue of capacity 64 claims
     * a slot of the first chunk, the second offer to a queue of capacity 2 resizes.
     */
    private static final List<String> CAPACITIES = List.of("64", "2");
    private static final String RESIZING = "2";

    private static final List<List<String>> MODES = List.of(
            List.of("-Xint"),
            List.of("-Xcomp", "-XX:-TieredCompilation", "-XX:CompileCommand=quiet",
                    "-XX:CompileCommand=compileonly,java.lang.RunQueue::*"),
            List.of());

    public static void main(String[] args) throws Exception {
        Path patch = Path.of("patch");
        writeHookedQueue(patch);
        List<String> failures = new ArrayList<>();
        int ran = 0;
        for (String capacity : CAPACITIES) {
            List<String> calls = record(patch, capacity);
            System.out.println("capacity " + capacity + ": hooked calls of the offer " + calls);
            if (calls.isEmpty()) {
                throw new RuntimeException("capacity " + capacity
                        + ": the offer made no hooked call");
            }
            if (capacity.equals(RESIZING) && !new HashSet<>(calls).equals(HOOKED)) {
                throw new RuntimeException("the resizing offer's calls " + calls
                        + " do not include every hooked accessor " + HOOKED);
            }
            for (List<String> mode : MODES) {
                for (int k = 1; k <= calls.size(); k++) {
                    String scenario = "capacity " + capacity + ", call " + k + " ("
                            + calls.get(k - 1) + ")";
                    String result = runChild(patch, mode, "throw", capacity, k, ran++);
                    System.out.println(mode + " " + scenario + ": " + result);
                    if (!result.equals("PASS")) {
                        failures.add(mode + " " + scenario + ": " + result);
                    }
                }
            }
        }
        if (!failures.isEmpty()) {
            throw new RuntimeException(failures.size() + " scenarios failed:\n"
                    + String.join("\n", failures));
        }
        System.out.println(ran + " scenarios passed");
    }

    /** The hooked calls, in order, of the second offer to a new queue of the given capacity. */
    private static List<String> record(Path patch, String capacity) throws Exception {
        String result = runChild(patch, List.of(), "record", capacity, 0, -1);
        if (!result.startsWith("CALLS")) {
            throw new RuntimeException("capacity " + capacity + ": recording run: " + result);
        }
        return Arrays.stream(result.split(" ")).skip(1).toList();
    }

    /** Runs one Child in a JVM of its own and returns its RESULT line without the prefix. */
    private static String runChild(Path patch, List<String> mode, String action, String capacity,
                                   int k, int n) throws Exception {
        List<String> cmd = new ArrayList<>(mode);
        cmd.addAll(List.of(
                "-Xbootclasspath/a:" + System.getProperty("test.classes"),
                "--patch-module", "java.base=" + patch.toAbsolutePath(),
                "--add-reads", "java.base=ALL-UNNAMED",
                "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                "-Dtest.timeout.factor=" + Utils.TIMEOUT_FACTOR,
                "MpscQueueInjectionTest$Child", action, capacity, Integer.toString(k)));
        Path log = Path.of("child-" + action + "-" + capacity + "-" + n + ".log");
        Process p = ProcessTools.createLimitedTestJavaProcessBuilder(cmd)
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!p.waitFor(CHILD_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            p.destroyForcibly().waitFor();
            return "the child did not end in " + CHILD_TIMEOUT_MS + " ms";
        }
        List<String> lines = Files.readAllLines(log);
        return lines.stream().filter(l -> l.startsWith("RESULT ")).findFirst()
                .map(l -> l.substring("RESULT ".length()))
                .orElse("no result line, exit " + p.exitValue() + ": " + lines);
    }

    /**
     * The running JDK's queue class with a call to Hook.hit(name) at the start of each hooked
     * accessor. A renamed accessor fails here.
     */
    private static void writeHookedQueue(Path patch) throws Exception {
        byte[] in;
        try (var s = Object.class.getResourceAsStream("/java/lang/RunQueue.class")) {
            in = s.readAllBytes();
        }
        ClassDesc hook = ClassDesc.of("MpscQueueInjectionTest$Hook");
        MethodTypeDesc hit = MethodTypeDesc.of(ConstantDescs.CD_void, ConstantDescs.CD_String);
        ClassFile cf = ClassFile.of();
        int[] hooked = new int[1];
        byte[] out = cf.transformClass(cf.parse(in), (clb, ce) -> {
            if (ce instanceof MethodModel mm && HOOKED.contains(mm.methodName().stringValue())) {
                String name = mm.methodName().stringValue();
                hooked[0]++;
                clb.transformMethod(mm, MethodTransform.transformingCode(new CodeTransform() {
                    @Override
                    public void atStart(CodeBuilder b) {
                        b.ldc(name);
                        b.invokestatic(hook, "hit", hit);
                    }
                    @Override
                    public void accept(CodeBuilder b, CodeElement e) {
                        b.with(e);
                    }
                }));
            } else {
                clb.with(ce);
            }
        });
        if (hooked[0] != HOOKED.size()) {
            throw new RuntimeException("hooked " + hooked[0] + " accessors, expected "
                    + HOOKED.size());
        }
        Path f = patch.resolve("java/lang/RunQueue.class");
        Files.createDirectories(f.getParent());
        Files.write(f, out);
    }

    /**
     * Called first by every hooked accessor. On the armed thread it records the accessor's
     * name, or throws at the armed call.
     */
    public static final class Hook {
        private static volatile Thread target;
        private static int countdown;
        private static List<String> recorded;
        static volatile boolean fired;

        static void record() {
            recorded = new ArrayList<>();
            target = Thread.currentThread();
        }

        static void arm(int k) {
            countdown = k;
            target = Thread.currentThread();
        }

        static List<String> disarm() {
            target = null;
            return recorded;
        }

        public static void hit(String accessor) {
            if (Thread.currentThread() != target) {
                return;
            }
            if (recorded != null) {
                recorded.add(accessor);
            } else if (--countdown == 0) {
                target = null;
                fired = true;
                throw new StackOverflowError("injected at the entry of ".concat(accessor));
            }
        }
    }

    /**
     * One run on a hooked queue, in a JVM of its own: offers a0, then a1 while the hook
     * records or is armed. Prints one RESULT line.
     */
    public static final class Child {
        public static void main(String[] a) throws Exception {
            boolean record = a[0].equals("record");
            int capacity = Integer.parseInt(a[1]);
            int k = Integer.parseInt(a[2]);
            Class<?> qc = Class.forName("java.lang.RunQueue");
            Constructor<?> ctor = qc.getDeclaredConstructor(int.class, int.class);
            ctor.setAccessible(true);
            Method offer = qc.getDeclaredMethod("offer", Object.class);
            Method take = qc.getDeclaredMethod("take", boolean.class);
            Method isEmpty = qc.getDeclaredMethod("isEmpty");
            for (var m : List.of(offer, take, isEmpty)) m.setAccessible(true);

            Object q = ctor.newInstance(capacity, capacity);
            offer.invoke(q, "a0");
            if (record) {
                Hook.record();
                offer.invoke(q, "a1");
                result("CALLS " + String.join(" ", Hook.disarm()));
            }
            Hook.arm(k);
            try {
                offer.invoke(q, "a1");
                result("FAIL: the armed offer returned (fired=" + Hook.fired + ")");
            } catch (InvocationTargetException e) {
                if (!(e.getCause() instanceof StackOverflowError) || !Hook.fired) {
                    result("FAIL: the armed offer threw " + e.getCause());
                }
            }
            Thread producer = Thread.ofPlatform().daemon().start(() -> {
                try {
                    for (String s : List.of("a2", "a3", "a4")) offer.invoke(q, s);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            producer.join(childTimeout(5_000));
            if (producer.isAlive()) {
                result("FAIL: offers after the error did not return");
            }
            List<Object> got = new ArrayList<>();
            Thread consumer = Thread.ofPlatform().daemon().start(() -> {
                try {
                    for (Object e; (e = take.invoke(q, true)) != null; ) got.add(e);
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            });
            consumer.join(childTimeout(10_000));
            if (consumer.isAlive()) {
                result("FAIL: the consumer did not return after " + got);
            }
            // the throwing offer may or may not have queued a1
            List<String> with = List.of("a0", "a1", "a2", "a3", "a4");
            List<String> without = List.of("a0", "a2", "a3", "a4");
            if (!(got.equals(with) || got.equals(without)) || !(boolean) isEmpty.invoke(q)) {
                result("FAIL: took " + got + ", expected " + with + " or " + without
                        + ", empty=" + isEmpty.invoke(q));
            }
            result("PASS");
        }

        // the child runs from the boot class path, where jdk.test.lib.Utils is not visible
        private static long childTimeout(long ms) {
            return (long) (ms * Double.parseDouble(System.getProperty("test.timeout.factor", "1.0")));
        }

        private static void result(String r) {
            System.out.println("RESULT " + r);
            System.exit(0);
        }
    }
}
